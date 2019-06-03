package ch.ethz.infsec.slicer

import ch.ethz.infsec.monitor.{Domain, IntegralValue, StringValue}
import ch.ethz.infsec.policy._
import ch.ethz.infsec.trace
import ch.ethz.infsec.trace.Tuple
import ch.ethz.infsec.policy.{Pred, VariableID}

import scala.collection.mutable
import scala.util.Random
import scala.util.hashing.MurmurHash3

class HypercubeSlicer(
                       val formula: Formula,
                       var heavy: IndexedSeq[(Int, Set[Domain])],
                       var shares: IndexedSeq[IndexedSeq[Int]],
                       val seed: Long = 1234) extends DataSlicer with Serializable {
  override type State = Array[Byte]

  private val parser = new SlicerParser

  // The number of variables with heavy hitters is limited to 30 because of the internal encoding
  // of variable sets as bit masks. A higher limit is probably unreasonable.
  require(heavy.count(x => x._1 >= 0) <= 30)
  require(heavy.forall{case (k, _) => k < 30})
  require(shares.length == (1 << heavy.count(x => x._1 >= 0)))
  require(shares.zipWithIndex.forall{case (s, _) => s.length == formula.freeVariables.size})

  val dimensions: Int = formula.freeVariables.size

  override val degree: Int = {
    val simpleShares = shares(0)
    if (simpleShares.isEmpty) 1 else simpleShares.product
  }

  private var seeds: Array[Array[Int]] = {
    val random = new Random(seed)
    // We enforce equal seeds for equal share combinations. This reduces the amount of duplication
    // if a variable is not sliced even in the "light" case.
    val distinctShares = shares.distinct
    val distinctSeeds = Array.fill(distinctShares.length){Array.fill(dimensions){random.nextInt()}}
    Array.tabulate(shares.length){ i =>
      val j = distinctShares.indexOf(shares(i))
      distinctSeeds(j)
    }
  }

  private var strides = calcStrides

  private def calcStrides: Array[Array[Int]] = {
    val strides = Array.fill(shares.length){Array.fill(dimensions)(1)}
    for ((s, h) <- strides.zipWithIndex)
      for (i <- 1 until dimensions)
        s(i) = s(i - 1) * shares(h)(i - 1)
    strides
  }

  private def hash(value: Domain, seed: Int): Int = value match {
    case IntegralValue(x) =>
      val lo = x.toInt
      val hi = (x >> 32).toInt
      MurmurHash3.finalizeHash(MurmurHash3.mixLast(MurmurHash3.mix(seed, lo), hi), 0)
    case StringValue(x) => MurmurHash3.stringHash(x, seed)
  }

  override def addSlicesOfValuation(valuation: Array[Domain], slices: mutable.HashSet[Int]) {
    //var internalSet = new mutable.HashSet[Int]()
    // Compute which variables hold heavy hitters. This determines the shares to use.
    // Additionally, we have to consider all combinations of variables that are not constrained by the valuation,
    // but that can take on heavy hitters in other valuations.
    var heavySet = 0
    var unconstrainedSet = 0
    var i = 0

    while (i < valuation.length) {
      val heavyMap = heavy(i)
      if (heavyMap._1 >= 0) {
        val value = valuation(i)
        if (value == null)
          unconstrainedSet += (1 << heavyMap._1)
        else if (heavyMap._2 contains value)
          heavySet += (1 << heavyMap._1)
      }
      i += 1
    }

    def addSlicesForHeavySet(heavySet: Int) {
      val theSeeds = seeds(heavySet)
      val theStrides = strides(heavySet)
      val theShares = shares(heavySet)

      // Select the coordinates for variables whose value is determined by the tuple.
      var sliceIndex = 0
      i = 0
      while (i < valuation.length) {
        val value = valuation(i)
        if (value != null)
          sliceIndex += theStrides(i) * Math.floorMod(hash(value, theSeeds(i)), theShares(i))

        i += 1
      }

      // All other other variables are broadcast in their dimension.
      def broadcast(i: Int, k: Int): Unit = {
        if (i < 0){
          slices += k
          //internalSet += k
        } else {
          val value = valuation(i)
          if (value == null)
            for (j <- 0 until theShares(i)) broadcast(i - 1, k + theStrides(i) * j)
          else
            broadcast(i - 1, k)
        }
      }

      broadcast(valuation.length - 1, sliceIndex)
    }

    // Iterate over all combinations of the unconstrained variables.
    def coverUnconstrained(m: Int, h: Int): Unit = {
      if (m == 0) addSlicesForHeavySet(h) else {
        if ((unconstrainedSet & m) != 0)
          coverUnconstrained(m >> 1, h | m)
        coverUnconstrained(m >> 1, h)
      }
    }

    if (valuation.isEmpty)
      addSlicesForHeavySet(heavySet)
    else
      coverUnconstrained(1 << (valuation.length - 1), heavySet)

    //println(">{(%s),(%s),(%s)}<".format("data,x,y", valuation.mkString(","), internalSet.mkString(",")))
  }

  // TODO(JS): Verify whether Monpoly enumerates free variable in DFS order.
  val variablesInOrder: Seq[VariableID] = formula.freeVariablesInOrder.distinct

  override def mkVerdictFilter(slice: Int)(verdict: Tuple): Boolean = {
    var heavySet = 0
    // Note: Variables are in a different order here, so i is not the index into "heavy"!
    var i = 0
    while (i < variablesInOrder.length) {
      val variableID = variablesInOrder(i).freeID
      val value = verdict(i)
      val heavyMap = heavy(variableID)
      if (heavyMap._1 >= 0 && (heavyMap._2 contains value))
        heavySet += (1 << heavyMap._1)
      i += 1
    }

    val theSeeds = seeds(heavySet)
    val theStrides = strides(heavySet)
    val theShares = shares(heavySet)

    var expectedSlice = 0
    i = 0
    while (i < variablesInOrder.length) {
      val variableID = variablesInOrder(i).freeID
      val value = verdict(i)
      expectedSlice += theStrides(variableID) * Math.floorMod(hash(value, theSeeds(variableID)), theShares(variableID))
      i += 1
    }

    slice == expectedSlice
  }

  override def setSlicer(record: trace.Record): Unit = super.setSlicer(record)

  override def getState(): State  = {
    if (pendingSlicer != null)
      pendingSlicer.toCharArray.map(_.toByte)
    else
      parser.stringify(heavy, shares, seeds).toCharArray.map(_.toByte)
  }

  def stringify(): String = {
    parser.stringify(heavy, shares, seeds)
  }

  override def restoreState(state: Option[State]): Unit = {
    var stringifiedSlicer: String = null

    state match {
      case Some(x) =>
        stringifiedSlicer = x.map(_.toChar).mkString("")
        parseSlicer(stringifiedSlicer)
       // println(">set_slicer %s<".format(stringifiedSlicer))
      case None => throw new Exception("No state to restore slicer from")
    }
  }

  private def parseSlicer(slicer: String): Unit = {
    val res = parser.parseSlicer(slicer)
    heavy = res._1
    shares = res._2
    seeds = res._3
    strides = calcStrides
  }

  override def updateState(state: Array[Byte]): Unit = parseSlicer(state.map(_.toChar).mkString(""))
}

object HypercubeSlicer {
  // TODO(JS): This is a temporary workaround until we properly support rigid/built-in predicates.
  private def isRigidRelation(relation: String): Boolean = relation.startsWith("__")

  def fromSimpleShares(formula: Formula, shares: Map[VariableID, Int]): HypercubeSlicer = {
    val heavy = Array.fill(formula.freeVariables.size){(-1, Set.empty: Set[Domain])}
    val sharesById: Map[Int, Int] = shares.map { case (v, e) => (v.freeID, e) }.withDefaultValue(1)
    val simpleShares = Array.tabulate(formula.freeVariables.size)(sharesById(_))
    new HypercubeSlicer(formula, heavy, Array[IndexedSeq[Int]](simpleShares))
  }

  def optimizeSingleSet(
      formula: Formula,
      degree: Int,
      statistics: Statistics,
      activeVariables: Set[Int]): Array[Int] = {

    require(degree >= 1)

    var bestCost: Double = Double.PositiveInfinity
    var bestMaxShare: Int = degree
    var bestShares: List[Int] = Nil

    def atomPartitions(atom: Pred[VariableID], shares: List[Int]): Double =
      atom.args.distinct.map {
        case Var(x) if x.isFree => shares(x.freeID).toDouble
        case _ => 1.0
      }.product

    // This is essentially Algorithm 1 from S. Chu, M. Balazinska and D. Suciu (2015), "From Theory to Practice:
    // Efficient Join Query Evaluation in a Parallel Database System", SIGMOD'15.
    // TODO(JS): Branch-and-bound?
    def search(remainingVars: Int, remainingShares: Int, shares: List[Int]): Unit =
      if (remainingVars >= 1) {
        val variable = remainingVars - 1
        val maxShare = if (activeVariables contains variable) remainingShares else 1
        for (p <- 1 to maxShare if remainingShares % p == 0)
          search(remainingVars - 1, remainingShares / p, p :: shares)
      } else {
        // TODO(JS): This cost function does not consider constant constraints nor non-linear atoms.
        val cost = formula.atoms.toSeq.filterNot(p => isRigidRelation(p.relation)).map((atom: Pred[VariableID]) =>
          statistics.relationSize(atom.relation) / atomPartitions(atom, shares)).sum
        val maxShare = shares.max

        if (cost < bestCost || (cost == bestCost && maxShare < bestMaxShare)) {
          bestShares = shares
          bestCost = cost
          bestMaxShare = maxShare
        }
      }

    search(formula.freeVariables.size, degree, Nil)
    bestShares.toArray
  }

  def optimize(formula: Formula, degree: Int, statistics: Statistics): HypercubeSlicer = {
    val heavy = Array.fill(formula.freeVariables.size){(-1, Set.empty: Set[Domain])}
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
      optimizeSingleSet(formula, degree, statistics, activeVariables)
    })

    val slicer = new HypercubeSlicer(formula, heavy, shares)
    //println(slicer.stringify())
    slicer
  }
}
