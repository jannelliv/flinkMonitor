package ch.ethz.infsec.trace

import java.io.FileWriter

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
        (Letter | Digit | CharIn("_[]/:\"-.,!(){}")).rep(min = 1).!
    )


    val Value: P[Domain] = P( Integer.map(IntegralValue(_)) | String.map(StringValue(_)) )
  }

  private val WhitespaceWrapper = WhitespaceApi.Wrapper(Token.Whitespace)
  import WhitespaceWrapper._

  private val Relation: P[Seq[Tuple]] = P( ("(" ~/ Token.Value.rep(sep = ",").map(_.toIndexedSeq) ~/ ")").rep )

  private val Database: P[Seq[(String, Seq[Tuple])]] = P( (Token.String ~/ Relation).rep )

  val OneEvent: P[Record] = P(("@" ~/ Token.IntegerDot ~/ Token.String ~/ "(" ~/ Token.Value.rep(sep = ",").map(_.toIndexedSeq) ~/ ")").map(x=>EventRecord(x._1,x._2,x._3)))
  val IndexedRecord: P[(Int,Record)] = P(Token.Whitespace ~/ Token.Integer.map(_.toInt) ~/ (OneEvent | Command) ~/ Token.Whitespace ~/ End )

  val Command: P[Record] = P(
    (Token.Whitespace ~/ ">" ~/ Token.String ~/ Token.CommandString ~/ "<" ~/ Token.Whitespace ~/ End).map {
      case (s, param) => CommandRecord(s, param)
  })

  val Event: P[(Long, Seq[Record])] = P(
    (Token.Whitespace ~/ "@" ~/ Token.IntegerDot ~/ Database ~/ ";".? ~/ Token.Whitespace ~/ End).map {
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
        case _ => println("Could not parse line: " + line)
      }
    }
  }

  override def terminate(f: Record => Unit): Unit = ()
}

class LiftProcessor(proc : Processor[String,String]) extends Processor[MonpolyRequest,MonpolyRequest] with Serializable{
  val proc2 = proc
  override type State = proc2.State

  override def isStateful: Boolean = proc2.isStateful

  override def getState: State = proc2.getState

  override def restoreState(state: Option[State]): Unit = proc2.restoreState(state)

/*  var started = false
  var tempF : FileWriter = null
*/

  override def process(in: MonpolyRequest, f: MonpolyRequest => Unit): Unit = {
/*    if(!started) {
      started = true
      tempF = new FileWriter("LiftProcessor.log",false)
    }
    tempF.write(in.in + "\n")
    tempF.flush()
*/
    in match {
      case c@CommandItem(a) => f(c)
      case EventItem(b) => proc2.process(b,x => f(EventItem(x)))
    }
  }

  override def terminate(f: MonpolyRequest => Unit): Unit = {
    proc2.terminate(x => f(EventItem(x)))
  }
  def accessInternalProcessor : Processor[String,String] = proc2
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

class MonpolyPrinter[T](markDatabaseEnd: Boolean) extends Processor[Either[Record,T], Either[MonpolyRequest,T]] with Serializable {
  protected var buffer = new ArrayBuffer[Record]()
  protected var delayBuffer = new ArrayBuffer[T]()

  override type State = (Vector[Record],Vector[T])


  override def getState: State = (buffer.toVector,delayBuffer.toVector)


  override def restoreState(state: Option[State]): Unit = {
    delayBuffer.clear()
    buffer.clear()
    state match {
      case Some((b,db)) => buffer ++= b; delayBuffer ++= db
      case None => ()
    }
  }

  override def process(record: Either[Record,T], f: Either[MonpolyRequest,T] => Unit) {
    record match {
      case Left(rec@CommandRecord(_, _)) => processCommand(rec, f)
      case Left(rec@EventRecord(_, _, _)) => processEvent(rec, f)
      case Right(value) if buffer.isEmpty => f(Right(value))
      case Right(value) => delayBuffer += value
    }
  }

  def processEvent(record: Record, f:  Either[MonpolyRequest,T] => Unit): Unit = {
    buffer += record
    if (record.isEndMarker)
      terminate(f)
  }

  def processCommand(record: Record, f:  Either[MonpolyRequest,T] => Unit) {
    buffer += record

    if (buffer.nonEmpty) {
      val str = new mutable.StringBuilder()
      str.append('>').append(buffer.head.command).append(" ").append(buffer.head.parameters).append('<')
      str.append('\n')
      f(Left(CommandItem(str.toString())))
      buffer.clear()
    }
  }

  override def terminate(f:  Either[MonpolyRequest,T] => Unit) {
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
      if (markDatabaseEnd) {
        str.append(';')
      }
      str.append('\n')
      f(Left(EventItem(str.toString())))
      buffer.clear()
    }
    for (i <- delayBuffer){
      f(Right(i))
    }
    delayBuffer.clear()
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

// TODO(JS): Refactor (shared code with DejavuFormat)
class KeyedMonpolyPrinter[K,T](markDatabaseEnd: Boolean) extends Processor[Either[(K, Record),T], Either[MonpolyRequest,T]] with Serializable {
  private val internalPrinter = new MonpolyPrinter[T](markDatabaseEnd)

  @transient @volatile private var numberOfEvents: Long = 0

  override type State = MonpolyPrinter[T]#State

  override def getState: MonpolyPrinter[T]#State = internalPrinter.getState

  override def restoreState(state: Option[MonpolyPrinter[T]#State]): Unit = {
    internalPrinter.restoreState(state)
    numberOfEvents = 0
  }

  override def process(in: Either[(K, Record),T], f: Either[MonpolyRequest,T] => Unit): Unit = {
    in match {
      case Left((_,rec)) => {
        if (!rec.isEndMarker)
          numberOfEvents += 1
        internalPrinter.process(Left(rec), f)
      }
      case Right(r) => internalPrinter.process(Right(r),f)
    }
  }

  override def terminate(f: Either[MonpolyRequest,T] => Unit): Unit = internalPrinter.terminate(f)

  // TODO(JS): This doesn't really belong here.
  override def getCustomCounter: Long = numberOfEvents
}

object MonpolyFormat extends TraceFormat {
  override def createParser(): MonpolyParser = new MonpolyParser()
}
