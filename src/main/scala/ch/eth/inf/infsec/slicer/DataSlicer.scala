package ch.eth.inf.infsec
package slicer

import ch.eth.inf.infsec.policy._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// NOTE(JS): Also performs basic filtering (constants).
abstract class DataSlicer extends Slicer with Serializable {
  //NOTE(SK): Since Flink does not allow for custom keying we need
  //find precisely those keys the do not collide. See docs for ColissionlessKeyGenerator
  val remapper:PartialFunction[Int,Int]

  def slicesOfValuation(valuation: Array[Option[Any]]): TraversableOnce[Int]

  def slicesOfTuple(relation: String, tuple: Tuple): ArrayBuffer[Int] = {
    // TODO(JS): Make this more readable/idiomatic.
    val slices = new ArrayBuffer[Int](degree)
    for (atom <- formula.atoms if atom.relation == relation) {
      var matches = true
      val valuation: Array[Option[Any]] = Array.fill(formula.freeVariables.size)(None)
      for ((term, value) <- atom.args.zip(tuple))
        term match {
          case ConstInteger(const) if const != value => matches = false
          case ConstString(const) if const != value => matches = false
          case Var(x) if x.isFree =>
            if (valuation(x.freeID).isEmpty)
              valuation(x.freeID) = Some(value)
            else if (valuation(x.freeID).get != value)
              matches = false
          case _ => ()
        }

      if (matches)
        slices.appendAll(slicesOfValuation(valuation))
    }
    slices
  }

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

  //override def apply(source: Stream[Event]): source.Self[(Int, Event)] =
  //  source.flatMap(e => {
  override def apply(e: Event): TraversableOnce[(Int, Event)] = {
    // TODO(JS): For efficiency, consider transposing the outermost layers of `slices`: relation -> slice id -> structure
    val slices = Array.fill(degree){
      val slice = new mutable.HashMap[String, ArrayBuffer[Tuple]]()
      for (relation <- e.structure.keys)
        slice(relation) = new ArrayBuffer()
      slice
    }

    for ((relation, data) <- e.structure)
      for (tuple <- data)
        for (i <- slicesOfTuple(relation, tuple))
          slices(i)(relation) += tuple

    for (i <- slices.indices)
      yield (remapper(i), Event(e.timestamp, slices(i)))
  }
}
