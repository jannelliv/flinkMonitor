package ch.eth.inf.infsec.trace

import ch.eth.inf.infsec.CloseableIterable

import scala.collection.mutable
import scala.io.Source

object MonpolyFormat extends LogReader {
  // TODO(JS): Do we allow empty relations? Is there a difference if the relation is not included in an event?
  // What if the relation is just a proposition?

  private val event = """(?s)@(\d+)(.*)""".r
  private val structure = """\s*?([A-Za-z]\w*)\s*((\(\s*?\)|\(\s*?\w+(\s*?\,\s*?\w+)*\s*?\))*)""".r

  // TODO(JS): Proper parsing of all value types. The nonEmpty filter is a kludge for propositional events.
  def parseTuple(str:String):Set[Tuple] =
    str.trim.tail.init.split("""\)\s*\(""").map(_.split(',').filter(_.nonEmpty).map(_.trim.toLong).toIndexedSeq).toSet

  def parseLine(str: String):Option[Event] = {
    try {
      val event(ts, db) = str
      val relations = structure.findAllMatchIn(db)
      val map = new mutable.HashMap[String, Set[Tuple]]()
      relations.foreach(m=> map(m.group(1))=parseTuple(m.group(2)))
      Some(Event(ts.toLong,map))
    } catch {
      case _:Exception => None
    }
  }

  override def readFile(fileName: String): CloseableIterable[Event] = new CloseableIterable[Event] {
    private val source = Source.fromFile(fileName)

    override def close(): Unit = source.close()

    // TODO(JS): This skips over unreadable lines. Should we add a strict mode?
    override def iterator: Iterator[Event] = source.getLines().flatMap(parseLine)
  }

  def printEvent(event: Event): String = {
    def appendValue(builder: mutable.StringBuilder, value: Any): Unit = value match {
      case s: String => builder.append('"').append(s).append('"')
      case x => builder.append(x)
    }

    val str = new mutable.StringBuilder()
    str.append('@').append(event.timestamp)
    for ((relation, data) <- event.structure) {
      str.append(' ').append(relation)
      for (tuple <- data) {
        str.append('(')
        if (tuple.nonEmpty) {
          appendValue(str, tuple.head)
          for (value <- tuple.tail)
            appendValue(str.append(','), value)
        }
        str.append(')')
      }
    }
    str.append('\n')
    str.mkString
  }
}
