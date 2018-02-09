package ch.eth.inf.infsec.slicer

import ch.eth.inf.infsec.policy._

import scala.collection.mutable.ArrayBuffer
import scala.util.{Random, hashing}

class HypercubeSlicer(
                       val formula: Formula,
                       val heavy: IndexedSeq[(Int, Set[Any])],
                       val shares: IndexedSeq[IndexedSeq[Int]],
                       val seed: Long = 1234) extends DataSlicer {

  // The number of variables with heavy hitters is limited to 30 because of the internal encoding
  // of variable sets as bit masks. A higher limit is probably unreasonable.
  require(heavy.count(x => x._1 >= 0) <= 30)
  require(heavy.forall{case (k, _) => k < 30})
  require(shares.length == (1 << heavy.count(x => x._1 >= 0)))
  require(shares.zipWithIndex.forall{case (s, h) => s.length == formula.freeVariables.size})

  val dimensions: Int = formula.freeVariables.size

  override val degree: Int = {
    val simpleShares = shares(0)
    if (simpleShares.isEmpty) 1 else simpleShares.product
  }

  override val remapper: PartialFunction[Int, Int] = ColissionlessKeyGenerator.getMapping(degree)

  private val seeds: Array[Array[Int]] = {
    val random = new Random(seed)
    Array.fill(shares.length){Array.fill(dimensions){random.nextInt()}}
  }

  private val strides: Array[Array[Int]] = {
    val strides = Array.fill(shares.length){Array.fill(dimensions)(1)}
    for ((s, h) <- strides.zipWithIndex)
      for (i <- 1 until dimensions)
        s(i) = s(i - 1) * shares(h)(i - 1)
    strides
  }

  // TODO(JS): Use a proper hash function.
  private def hash(value: Any, seed: Int): Int = hashing.byteswap32(value.## ^ seed)

  // TODO(JS): Profile and optimize
  override def slicesOfValuation(valuation: Array[Option[Any]]): Iterable[Int] = {
    var heavySet = 0
    for ((value, i) <- valuation.zipWithIndex if heavy(i)._1 >= 0) {
      val heavyMap = heavy(i)
      if (value.isDefined && (heavyMap._2 contains value.get))
        heavySet += (1 << heavyMap._1)
    }
    val theSeeds = seeds(heavySet)
    val theStrides = strides(heavySet)
    val theShares = shares(heavySet)

    var unconstrained: List[Int] = Nil
    var sliceIndex = 0
    for ((value, i) <- valuation.zipWithIndex) {
      if (value.isEmpty)
        unconstrained ::= i
      else
        sliceIndex += theStrides(i) * Math.floorMod(hash(value.get, theSeeds(i)), theShares(i))
    }

    val slices = new ArrayBuffer[Int](degree)
    def broadcast(us: List[Int], k: Int): Unit = us match {
      case Nil => slices += k
      case u :: ust => for (j <- 0 until theShares(u)) broadcast(ust, k + theStrides(u) * j)
    }
    broadcast(unconstrained, sliceIndex)

    slices
  }
}

object HypercubeSlicer {
  // TODO(JS): This is a temporary workaround until we properly support rigid/built-in predicates.
  private def isRigidRelation(relation: String): Boolean = relation.startsWith("__")

  def optimizeSingleSet(
      formula: Formula,
      degreeExp: Int,
      statistics: Statistics,
      activeVariables: Set[Int]): Array[Int] = {

    require(degreeExp >= 0 && degreeExp < 31)

    var bestCost: Double = Double.PositiveInfinity
    var bestConfig: List[Int] = Nil

    def atomPartitions(atom: Pred[VariableID], config: List[Int]): Double =
      atom.args.distinct.map {
        case Var(x) if x.isFree => (1 << config(x.freeID)).toDouble
        case _ => 1.0
      }.product

    // TODO(JS): Branch-and-bound?
    def search(remainingVars: Int, remainingExp: Int, config: List[Int]): Unit =
      if (remainingVars >= 1) {
        val variable = remainingVars - 1
        val maxExp = if (activeVariables contains variable) remainingExp else 0
        for (e <- 0 to maxExp)
          search(remainingVars - 1, remainingExp - e, e :: config)
      } else {
        // TODO(JS): This cost function does not consider constant constraints nor non-linear atoms.
        val cost = formula.atoms.toSeq.filterNot(p => isRigidRelation(p.relation)).map((atom: Pred[VariableID]) =>
          statistics.relationSize(atom.relation) / atomPartitions(atom, config)).sum

        if (cost < bestCost) {
          bestConfig = config
          bestCost = cost
        }
      }

    search(formula.freeVariables.size, degreeExp, Nil)
    bestConfig.map(e => 1 << e).toArray
  }

  def optimize(formula: Formula, degreeExp: Int, statistics: Statistics): HypercubeSlicer = {
    val heavy = Array.fill(formula.freeVariables.size){(-1, Set.empty: Set[Any])}
    var heavyIndex = 0
    for (atom <- formula.atoms) {
      for ((Var(v), i) <- atom.args.zipWithIndex if v.isFree) {
        val hitters = statistics.heavyHitters(atom.relation, i)
        if (hitters.nonEmpty) {
          if (heavy(v.freeID)._1 < 0) {
            heavy(v.freeID) = (heavyIndex, hitters)
            heavyIndex += 1
          } else {
            val (index, old) = heavy(v.freeID)
            heavy(v.freeID) = (index, old.union(hitters))
          }
        }
      }
    }

    val heavyCount = heavy.count(x => x._1 >= 0)
    if (heavyCount > 30)
      throw new IllegalArgumentException("Too many variables with heavy hitters")

    val shares: Array[IndexedSeq[Int]] = Array.tabulate(1 << heavyCount)(h => {
      val activeVariables = (0 until formula.freeVariables.size).filter(v => (h & (1 << heavy(v)._1)) == 0).toSet
      optimizeSingleSet(formula, degreeExp, statistics, activeVariables)
    })

    new HypercubeSlicer(formula, heavy, shares)
  }
}
