package ch.eth.inf.infsec.trace

import ch.eth.inf.infsec.{Processor, StatelessProcessor}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

class MonpolyParser extends StatelessProcessor[String, Record] with Serializable {
  // TODO(JS): Do we allow empty relations? Is there a difference if the relation is not included in an event?
  // What if the relation is just a proposition?

  protected val event: Regex = """(?s)@(\d+)(.*)""".r
  protected val structure: Regex = """\s*?([A-Za-z]\w*)\s*((\(\s*?\)|\(\s*?\w+(\s*?\,\s*?\w+)*\s*?\))*)""".r

  protected val buffer = new ArrayBuffer[Record]()

  // TODO(JS): This skips over unreadable lines. Should we add a strict mode?
  override def process(line: String, f: Record => Unit) {
    try {
      val event(ts, db) = line
      val timestamp = ts.toLong
      for (m <- structure.findAllMatchIn(db))
        for (data <- MonpolyParser.parseTuple(m.group(2)))
          buffer += Record(timestamp, m.group(1), data)
      buffer += Record.markEnd(timestamp)
      buffer.foreach(f)
    } catch {
      case _: Exception => ()
    }
    buffer.clear()
  }

  override def terminate(f: Record => Unit) { }
}

object MonpolyParser {
  // TODO(JS): Proper parsing of all value types. The nonEmpty filter is a kludge for propositional events.
  def parseTuple(str:String):Set[Tuple] =
    str.trim.tail.init.split("""\)\s*\(""").map(_.split(',').filter(_.nonEmpty)
      .map(x => IntegralValue(x.trim.toLong)).toIndexedSeq)
      .toSet
}

class MonpolyPrinter extends Processor[Record, String] with Serializable {
  protected var buffer = new ArrayBuffer[Record]()

  override type State = ArrayBuffer[Record]

  override def getState: ArrayBuffer[Record] = buffer

  override def restoreState(state: Option[ArrayBuffer[Record]]): Unit = state match {
    case Some(b) => buffer = b
    case None => buffer.clear()
  }

  protected def appendValue(builder: mutable.StringBuilder, value: Domain): Unit = value match {
    case StringValue(s) => builder.append('"').append(s).append('"')
    case IntegralValue(x) => builder.append(x)
  }

  override def process(record: Record, f: String => Unit) {
    buffer += record
    if (record.isEndMarker)
      terminate(f)
  }

  override def terminate(f: String => Unit) {
    if (buffer.nonEmpty) {
      val str = new mutable.StringBuilder()
      str.append('@').append(buffer.head.timestamp)
      for ((label, records) <- buffer.groupBy(_.label) if label.nonEmpty) {
        str.append(' ').append(label)
        for (record <- records) {
          str.append('(')
          if (record.data.nonEmpty) {
            appendValue(str, record.data.head)
            for (value <- record.data.tail)
              appendValue(str.append(','), value)
          }
          str.append(')')
        }
      }
      str.append('\n')
      f(str.toString())
      buffer.clear()
    }
  }
}

object MonpolyFormat extends TraceFormat {
  override def createParser(): MonpolyParser = new MonpolyParser()
}
