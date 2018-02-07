package ch.eth.inf.infsec

package object trace {
  // TODO(JS): Type of data domain?
  // TODO(JS): Consider using a more specialized container type, e.g. Array.
  type Tuple = IndexedSeq[Any]

  case class Event(timestamp: Long, structure: collection.Map[String, Iterable[Tuple]]) {
    override def toString: String = MonpolyFormat.printEvent(this)
  }

  trait LogReader {
    def readFile(fileName: String): CloseableIterable[Event]
  }
}
