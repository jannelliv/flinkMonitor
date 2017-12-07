package ch.eth.inf.infsec.slicer

import ch.eth.inf.infsec.policy._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Random, hashing}

class HypercubeSlicer(
    val formula: Formula,
    val shares: IndexedSeq[Int],
    val seed: Long = 1234) extends DataSlicer {

  override val degree: Int = if (shares.isEmpty) 1 else shares.product

  private val seeds: Array[Int] = {
    val random = new Random(seed)
    Array.fill(shares.length){random.nextInt()}
  }

  private val strides: Array[Int] = {
    val strides = Array.fill(shares.length)(1)
    for (i <- 1 until shares.length)
      strides(i) = strides(i - 1) * shares(i - 1)
    strides
  }

  // TODO(JS): Use a proper hash function.
  private def hash(value: Any, seed: Int): Int = hashing.byteswap32(value.## ^ seed)

  override def slicesOfValuation(valuation: Array[Option[Any]]): Iterable[Int] = {
    var unconstrained: List[Int] = Nil
    var sliceIndex = 0
    for ((value, i) <- valuation.zipWithIndex) {
      if (value.isEmpty)
        unconstrained ::= i
      else
        sliceIndex += strides(i) * Math.floorMod(hash(value.get, seeds(i)), shares(i))
    }

    val slices = new ArrayBuffer[Int](degree)
    def broadcast(us: List[Int], k: Int): Unit = us match {
      case Nil => slices += k
      case u :: ust => for (j <- 0 until shares(u)) broadcast(ust, k + strides(u) * j)
    }
    broadcast(unconstrained, sliceIndex)

    slices
  }
}

object HypercubeSlicer {
  def optimize(formula: Formula, degreeExp: Int, statistics: Statistics): HypercubeSlicer = {
    require(degreeExp >= 0 && degreeExp < 31)

    var bestCost: Double = Double.PositiveInfinity
    var bestConfig: List[Int] = Nil

    def atomPartitions(atom: Pred, config: List[Int]): Double =
      atom.args.toSet.map((t: Term) => t match {
        case Free(i, _) => (1 << config(i)).toDouble
        case _ => 1.0
      }).product

    // TODO(JS): Branch-and-bound?
    def search(remainingVars: Int, remainingExp: Int, config: List[Int]): Unit =
      if (remainingVars >= 1) {
        for (e <- 0 to remainingExp)
          search(remainingVars - 1, remainingExp - e, e :: config)
      } else {
        // TODO(JS): This cost function does not consider constant constraints nor non-linear atoms.
        val cost = formula.atoms.map((atom: Pred) =>
          statistics.relationSize(atom.relation) / atomPartitions(atom, config)).sum
        if (cost < bestCost) {
          bestConfig = config
          bestCost = cost
        }
      }

    search(formula.freeVariables.size, degreeExp, Nil)
    val shares = bestConfig.map(e => 1 << e).toArray
    new HypercubeSlicer(formula, shares)
  }
}
