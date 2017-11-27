package ch.eth.inf.infsec.slicer

import ch.eth.inf.infsec.Stream

trait Slicer {
  val formula: Formula
  val degree: Int

  def apply(source: Stream[Event]): source.Self[(Int, Event)]
}

// TODO(JS): Move to different package, this type is not specific to slicing.
case class Event(timestamp: Long, structure: collection.Map[String, Iterable[Tuple]]) {
  override def toString: String = s"@$timestamp: $structure"
}
