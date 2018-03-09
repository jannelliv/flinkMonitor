package ch.eth.inf.infsec
package slicer

import ch.eth.inf.infsec.policy._
import ch.eth.inf.infsec.trace.{Domain, Record}

import scala.collection.mutable

// NOTE(JS): Also performs basic filtering (constants).
abstract class DataSlicer extends StatelessProcessor[Record, (Int, Record)] {
  val formula: Formula
  val degree: Int

  //NOTE(SK): Since Flink does not allow for custom keying we need
  //find precisely those keys the do not collide. See docs for ColissionlessKeyGenerator
  // TODO(JS): Move out of slicing logic.
  val remapper:PartialFunction[Int,Int]

  def addSlicesOfValuation(valuation: Array[Domain], slices: mutable.HashSet[Int])

  def slicesOfValuation(valuation: Array[Domain]): collection.Set[Int] = {
    val slices = new mutable.HashSet[Int]()
    addSlicesOfValuation(valuation, slices)
    slices
  }

  // These arrays are reused in process() for performance.
  // We have to initialize them lazily, because they depend on properties provided by the implementing subclass.
  private var remapped: Array[Int] = _
  private var atoms: Array[Pred[VariableID]] = _
  private var valuation: Array[Domain] = _

  override def process(record: Record, f: ((Int, Record)) => Unit) {
    if (remapped == null) {
      remapped = Array.tabulate(degree)(remapper(_))
      atoms = formula.atoms.toArray
      valuation = Array.fill(formula.freeVariables.size)(null)
    }

    // The end of a timepoint is always broadcast to all slices.
    if (record.isEndMarker) {
      var i = 0
      while (i < degree) {
        f((remapped(i), record))
        i += 1
      }
      return
    }

    val slices = new mutable.HashSet[Int]()

    // The set of relevant slices is the union of the slices for each atom.
    var i = 0
    while (i < atoms.length) {
      val atom = atoms(i)
      if (atom.relation == record.label && atom.args.lengthCompare(record.data.length) == 0) {
        var matches = true
        var j = 0
        while (j < valuation.length) {
          valuation(j) = null
          j += 1
        }

        // Determine whether the tuple matches the atom, and compute the induced valuation of free variables.
        j = 0
        while (j < record.data.length) {
          val term = atom.args(j)
          val value = record.data(j)
          term match {
            case Const(c) if c != value => matches = false
            case Var(x) if x.isFree =>
              if (valuation(x.freeID) == null)
                valuation(x.freeID) = value
              else if (valuation(x.freeID) != value)
                matches = false
            case _ => ()
          }
          j += 1
        }

        if (matches)
          addSlicesOfValuation(valuation, slices)
      }
      i += 1
    }

    slices.foreach(s => f(remapped(s), record))
  }

  override def terminate(f: ((Int, Record)) => Unit) { }

//  override def apply(source: Stream[Event])(implicit in:TypeInfo[Event], out:TypeInfo[(Int, Event)]): source.Self[(Int, Event)] = {
//    source.flatMap(e => {
//      // TODO(JS): For efficiency, consider transposing the outermost layers of `slices`: relation -> slice id -> structure
//      val slices = Array.fill(degree) {
//        val slice = new mutable.HashMap[String, ArrayBuffer[Tuple]]()
//        for (relation <- e.structure.keys)
//          slice(relation) = new ArrayBuffer()
//        slice
//      }
//
//      for ((relation, data) <- e.structure)
//        for (tuple <- data)
//          for (i <- slicesOfTuple(relation, tuple))
//            slices(i)(relation) += tuple
//
//      for (i <- slices.indices)
//        yield (i, Event(e.timestamp, slices(i)))
//    })
//  }
}
