package ch.eth.inf.infsec
package slicer

import ch.eth.inf.infsec.policy._
import ch.eth.inf.infsec.trace.{Domain, Record}

import scala.collection.mutable.ArrayBuffer

// NOTE(JS): Also performs basic filtering (constants).
abstract class DataSlicer extends StatelessProcessor[Record, (Int, Record)] {
  val formula: Formula
  val degree: Int

  //NOTE(SK): Since Flink does not allow for custom keying we need
  //find precisely those keys the do not collide. See docs for ColissionlessKeyGenerator
  // TODO(JS): Move out of slicing logic.
  val remapper:PartialFunction[Int,Int]

  def slicesOfValuation(valuation: Array[Option[Domain]]): TraversableOnce[Int]

  override def process(record: Record, f: ((Int, Record)) => Unit) {
    if (record.isEndMarker) {
      (0 until degree).map(remapper(_)).foreach(f(_, record))
      return
    }

    val slices = new ArrayBuffer[Int](degree)
    for (atom <- formula.atoms if atom.relation == record.label) {
      var matches = true
      val valuation: Array[Option[Domain]] = Array.fill(formula.freeVariables.size)(None)
      for ((term, value) <- atom.args.zip(record.data))
        term match {
          case Const(c) if c != value => matches = false
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
    slices.distinct.foreach(f(_, record))
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
