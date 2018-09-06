package ch.eth.inf.infsec.trace

import ch.eth.inf.infsec.monitor.{CommandItem, EventItem, MonpolyRequest}
import ch.eth.inf.infsec.{Processor, StatelessProcessor}
import fastparse.WhitespaceApi
import fastparse.noApi._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

// TODO(JS): Should use a signature to determine the types of values.

// NOTE(JS): Our parser is stricter than Monpoly's: We expect exactly one timepoint per line.

object MonpolyParsers {

  private object Token {
    import fastparse.all._

    val LineComment: P0 = P( "#" ~/ CharsWhile(c => c != '\n' && c != '\r'))
    val Whitespace: P0 = P( NoTrace((CharsWhileIn(" \t\n\r") | LineComment).rep) )

    val Letter: P0 = P( CharIn('a' to 'z', 'A' to 'Z') )
    val Digit: P0 = P( CharIn('0' to '9') )

    // NOTE(JS): The current Log_parser of Monpoly does not support negative integers.
    val Integer: P[Long] = P( ("-".? ~ Digit.rep(min = 1)).!.map(_.toLong) )
    // TODO(JS): Floating-point timestamps?
    val IntegerDot: P[Long] = P( Integer ~ ".".? )
    val String: P[String] = P(
      "\"" ~/ CharsWhile(_ != '"').! ~/ "\"" |
      (Letter | Digit | CharIn("_[]/:-.!")).rep(min = 1).!
    )

    val CommandString: P[String] = P(
      "\"" ~/ CharsWhile(_ != '"').! ~/ "\"" |
        (Letter | Digit | CharIn("_[]/:-.,!(){}")).rep(min = 1).!
    )


    val Value: P[Domain] = P( Integer.map(IntegralValue(_)) | String.map(StringValue(_)) )
  }

  private val WhitespaceWrapper = WhitespaceApi.Wrapper(Token.Whitespace)
  import WhitespaceWrapper._

  private val Relation: P[Seq[Tuple]] = P( ("(" ~/ Token.Value.rep(sep = ",").map(_.toIndexedSeq) ~/ ")").rep )

  private val Database: P[Seq[(String, Seq[Tuple])]] = P( (Token.String ~/ Relation).rep )

  val Command: P[Record] = P(
    (Token.Whitespace ~/ ">" ~/ Token.String ~/ Token.CommandString ~/ "<" ~/ Token.Whitespace ~/ End).map {
      case (s, param) => CommandRecord(s, param)
  })

  val Event: P[(Long, Seq[Record])] = P(
    (Token.Whitespace ~/ "@" ~/ Token.IntegerDot ~/ Database ~/ Token.Whitespace ~/ End).map {
      case (ts, db) =>
      (ts, db.flatMap {
        case (rel, data) => data.map(t => EventRecord(ts, rel, t))
      })
  })

  val Verdict: P[(Long, Long, Seq[Tuple])] = P(
    Token.Whitespace ~/ "@" ~/ Token.IntegerDot ~/ "(time point" ~/ Token.Integer ~/ "):" ~/
      ("true" ~ PassWith(Seq(Tuple())) | Relation) ~ Token.Whitespace ~ End
  )
}

class MonpolyParser extends StatelessProcessor[String, Record] with Serializable {
  // TODO(JS): Do we allow empty relations? Is there a difference if the relation is not included in an event?
  // What if the relation is just a proposition?

  // TODO(JS): This skips over unreadable lines. Should we add a strict mode?
  override def process(line: String, f: Record => Unit): Unit = {
    var skipped = false
    MonpolyParsers.Event.parse(line) match {
      case Parsed.Success((timestamp, records), _) =>
        records.foreach(f)
        f(Record.markEnd(timestamp))
      case _ => skipped = true
    }
    if(skipped) {
      MonpolyParsers.Command.parse(line) match {
        case Parsed.Success(record, _) =>
          f(record)
        case _ => ()
      }
    }
  }

  override def terminate(f: Record => Unit): Unit = ()
}

class MonpolyVerdictFilter(var mkFilter: Int => Tuple => Boolean)
    extends Processor[String, String] with Serializable {
  override type State = Array[Byte]

  private var currentSlicer : Array[Byte] = _
  private var pendingSlicer : Array[Byte] = _

  private var pred: Tuple => Boolean = mkFilter(0)

  def updateProcessingFunction(f: Int => Tuple => Boolean): Unit = {
    mkFilter = f
  }

  override def setParallelInstanceIndex(instance: Int): Unit = {
    pred = mkFilter(instance)
  }

  override def process(in: String, f: String => Unit): Unit = MonpolyParsers.Verdict.parse(in) match {
    case Parsed.Success((timestamp, timepoint, rel), _) =>
      val out = new mutable.StringBuilder("@")
      out.append(timestamp)
      out.append(". (time point ")
      out.append(timepoint)
      out.append("):")
      var nonEmpty = false
      if (rel.size == 1 && rel.head.isEmpty) {
        out.append(" true")
        nonEmpty = true
      } else {
        for (tuple <- rel if pred(tuple)) {
          out.append(" (")
          MonpolyPrinter.appendValue(out, tuple.head)
          for (value <- tuple.tail)
            MonpolyPrinter.appendValue(out.append(','), value)
          out.append(')')
          nonEmpty = true
        }
      }
      if (nonEmpty) {
        f(out.toString())
      }else println("EMPTY, SHOULD NOT HAPPEN (FREQUENTLY)")

    case _ => ()
  }

  override def terminate(f: String => Unit): Unit = ()

  override def getState: Array[Byte] = {
    if(this.pendingSlicer != null) this.pendingSlicer
    else this.currentSlicer
  }

  override def restoreState(state: Option[Array[Byte]]): Unit = {
    state match {
      case Some(x) => this.currentSlicer = x
      case None =>
    }
  }

  def setCurrent(slicer: Array[Byte]): Unit = this.currentSlicer = slicer
  def updatePending(slicer: Array[Byte]): Unit = this.pendingSlicer = slicer
}

class MonpolyPrinter extends Processor[Record, MonpolyRequest] with Serializable {
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

  override def process(record: Record, f: MonpolyRequest => Unit) {
    record match {
      case CommandRecord(record.command, record.parameters) => processCommand(record, f)
      case EventRecord(record.timestamp, record.label, record.data) => processEvent(record, f)
    }
  }

  def processEvent(record: Record, f: MonpolyRequest => Unit): Unit = {
    buffer += record
    if (record.isEndMarker)
      terminate(f)
  }

  def processCommand(record: Record, f: MonpolyRequest => Unit) {
    buffer += record

    if (buffer.nonEmpty) {
      val str = new mutable.StringBuilder()
      str.append('>').append(buffer.head.command).append(" ").append(buffer.head.parameters).append('<')
      str.append('\n')
      f(CommandItem(str.toString()))
      buffer.clear()
    }
  }

  override def terminate(f: MonpolyRequest => Unit) {
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
      f(EventItem(str.toString()))
      buffer.clear()
    }
  }
}

object MonpolyPrinter {
  private val PlainString: Regex = """[a-zA-Z0-9_\[\]/:\-.!]*""".r

  def appendValue(builder: mutable.StringBuilder, value: Domain): Unit = value match {
    case StringValue(s) => s match {
      case PlainString() => builder.append(s)
      case _ => builder.append('"').append(s).append('"')
    }
    case IntegralValue(x) => builder.append(x)
  }
}

class KeyedMonpolyPrinter[K] extends Processor[(K, Record), MonpolyRequest] with Serializable {
  private val internalPrinter = new MonpolyPrinter

  @transient @volatile private var numberOfEvents: Long = 0

  override type State = MonpolyPrinter#State

  override def getState: MonpolyPrinter#State = internalPrinter.getState

  override def restoreState(state: Option[MonpolyPrinter#State]): Unit = {
    internalPrinter.restoreState(state)
    numberOfEvents = 0
  }

  override def process(in: (K, Record), f: MonpolyRequest => Unit): Unit = {
    if (!in._2.isEndMarker)
      numberOfEvents += 1
    internalPrinter.process(in._2, f)
  }

  override def terminate(f: MonpolyRequest => Unit): Unit = internalPrinter.terminate(f)

  // TODO(JS): This doesn't really belong here.
  override def getCustomCounter: Long = numberOfEvents
}

object MonpolyFormat extends TraceFormat {
  override def createParser(): MonpolyParser = new MonpolyParser()
}
