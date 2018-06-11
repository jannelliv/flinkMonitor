package ch.eth.inf.infsec.trace

import ch.eth.inf.infsec.{Processor, StatelessProcessor}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

class MonpolyParser extends StatelessProcessor[String, Record] with Serializable {
  // TODO(JS): Do we allow empty relations? Is there a difference if the relation is not included in an event?
  // What if the relation is just a proposition?

  protected val event: Regex = """(?s)@(\d+)(.*)""".r
  protected val structure: Regex = """\s*?([A-Za-z]\w*)\s*((\s*\(\s*?\)|\s*\(\s*?-?\d+(\s*?\,\s*?-?\d+)*\s*?\))*)""".r

  protected val buffer = new ArrayBuffer[Record]()

  // TODO(JS): This skips over unreadable lines. Should we add a strict mode?
  override def process(line: String, f: Record => Unit) {
    try {
      val event(ts, db) = line
      val timestamp = ts.toLong
      for (m <- structure.findAllMatchIn(db))
        for (data <- MonpolyParser.parseRelation(m.group(2)))
          buffer += Record(timestamp, m.group(1), data)
      buffer += Record.markEnd(timestamp)
      buffer.foreach(f)
    } catch {
      // TODO(JS): Be more precise with the exceptions that we want to ignore.
      case _: Exception => ()
    }
    buffer.clear()
  }

  override def terminate(f: Record => Unit) { }
}

object MonpolyParser {
  // TODO(JS): Proper parsing of all value types. The nonEmpty filter is a kludge for propositional events.
  def parseRelation(str:String):Set[Tuple] =
    str.trim.tail.init.split("""\)\s*\(""").map(_.split(',').filter(_.nonEmpty)
      .map(x => IntegralValue(x.trim.toLong)).toIndexedSeq)
      .toSet
}

class MonpolyVerdictFilter(mkFilter: Int => Tuple => Boolean) extends StatelessProcessor[String, String] with Serializable {
  protected val verdict: Regex = """(?s)(@\d+\.\s*\(time point \d+\):)(.*)""".r
  protected val relation: Regex = """\s*(true|(\s*\(\s*-?\d+(\s*,\s*-?\d+)*\s*\))*)\s*""".r

  private var pred: Tuple => Boolean = mkFilter(0)

  override def setParallelInstanceIndex(instance: Int): Unit = {
    pred = mkFilter(instance)
  }

  override def process(in: String, f: String => Unit): Unit = {
    try {
      val verdict(prefix, rel) = in
      val relation(body, _*) = rel

      val out = new mutable.StringBuilder(prefix)
      var nonEmpty = false

      if (body == "true") {
        out.append(rel)
        nonEmpty = true
      } else {
        for (data <- MonpolyParser.parseRelation(rel) if pred(data)) {
          out.append(" (")
          MonpolyPrinter.appendValue(out, data.head)
          for (value <- data.tail)
            MonpolyPrinter.appendValue(out.append(','), value)
          out.append(')')
          nonEmpty = true
        }
      }

      if (nonEmpty)
        f(out.toString())
    } catch {
      // TODO(JS): Be more precise with the exceptions that we want to ignore.
      case _: Exception => ()
    }
  }

  override def terminate(f: String => Unit): Unit = ()
}

class MonpolyPrinter extends Processor[Record, String] with Serializable {
  protected var buffer = new ArrayBuffer[Record]()

  override type State = Vector[Record]

  override def getState: Vector[Record] = buffer.toVector

  override def restoreState(state: Option[Vector[Record]]): Unit = {
    buffer.clear()
    state match {
      case Some(b) => buffer ++= b
      case None => ()
    }
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
            MonpolyPrinter.appendValue(str, record.data.head)
            for (value <- record.data.tail)
              MonpolyPrinter.appendValue(str.append(','), value)
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

object MonpolyPrinter {
  def appendValue(builder: mutable.StringBuilder, value: Domain): Unit = value match {
    case StringValue(s) => builder.append('"').append(s).append('"')
    case IntegralValue(x) => builder.append(x)
  }
}

class KeyedMonpolyPrinter[K] extends Processor[(K, Record), String] with Serializable {
  private val internalPrinter = new MonpolyPrinter

  override type State = MonpolyPrinter#State

  override def getState: MonpolyPrinter#State = internalPrinter.getState

  override def restoreState(state: Option[MonpolyPrinter#State]): Unit = internalPrinter.restoreState(state)

  override def process(in: (K, Record), f: String => Unit): Unit = internalPrinter.process(in._2, f)

  override def terminate(f: String => Unit): Unit = internalPrinter.terminate(f)
}

object MonpolyFormat extends TraceFormat {
  override def createParser(): MonpolyParser = new MonpolyParser()
}
