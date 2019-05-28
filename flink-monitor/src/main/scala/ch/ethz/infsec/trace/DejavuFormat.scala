package ch.ethz.infsec.trace

import ch.ethz.infsec.monitor._
import ch.ethz.infsec.{Processor, StatelessProcessor}
import fastparse.WhitespaceApi
import fastparse.noApi._


import scala.collection.mutable

object DejavuParsers{
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

    val Value: P[Domain] = P( Integer.map(IntegralValue(_)) | String.map(StringValue(_)) )
  }

  private val WhitespaceWrapper = WhitespaceApi.Wrapper(Token.Whitespace)
  import WhitespaceWrapper._

  private val Param: P[Tuple] = P(("," ~/ Token.Whitespace ~/ Token.Value).rep().map(_.toIndexedSeq))

  val Relation: P[(String,Tuple)] = P(Token.Whitespace ~/ Token.String ~/ Token.Whitespace ~/  Param ~/ Token.Whitespace ~/  End)
}

class DejavuParser extends  StatelessProcessor[String, Record] with Serializable{
  override def process(in: String, f: Record => Unit): Unit = {

    DejavuParsers.Relation.parse(in) match {
      case Parsed.Success(tup, _) =>
        if(!tup._1.equals(DejavuPrinter.DUMMY.label))
          f(EventRecord(0,tup._1,tup._2))
        f(Record.markEnd(0))
      case _ => ()
    }
  }

  override def terminate(f: Record => Unit): Unit = ()
}

class DejavuVerdictFilter extends StatelessProcessor[String,String]{

  override def process(in: String, f: String => Unit): Unit = {
    if (in!=null && in.length>0) {
      f(in.substring(DejavuProcess.VIOLATION_PREFIX.length, in.length - 1))
    }
  }

  override def terminate(f: String => Unit): Unit = ()
}

class DejavuPrinter extends Processor[Record, DejavuRequest] with Serializable {
  override type State = Option[Option[Record]]
  protected var rec: State = None
  /*
    State semantics:
    None = Empty trace
    Some(None) = Empty time point
    Some(Some(r)) = event r

   */


  override def getState: Option[Option[Record]] = rec

  override def restoreState(state: Option[State]): Unit = state  match {
    case Some(r) => rec = r
    case None => rec = None
  }

  // DejaVu ignores commands
  def processCommand(record: Record, f: DejavuRequest => Unit): Unit = ()

  def processEvent(record: Record, f: DejavuRequest => Unit): Unit = {
    if (record.isEndMarker) {
      rec match {
        case None => rec = Some(None)
        case _ => ()
      }
      terminate(f)
    }
    else
      rec match {
        case None => rec = Some(Some(record))
        case _ => throw new IllegalStateException("More than one event per database")
      }
  }


  override def process(record: Record, f: DejavuRequest => Unit): Unit = {
    record match {
      case CommandRecord(record.command, record.parameters) => processCommand(record, f)
      case EventRecord(record.timestamp, record.label, record.data) => processEvent(record, f)
    }
  }

  // Dejavu ignores timestamps
  def writetoCSV(r: Record): String = {
    val str = new mutable.StringBuilder()
    str.append(r.label)
    if(r.data.nonEmpty){
      for (d <- r.data){
        str.append(", ")
        str.append(d.toString)
      }
    }
    str.toString()
  }


  override def terminate(f: DejavuRequest => Unit): Unit = {
    rec match {
      case Some(Some(r)) => f(DejavuEventItem(writetoCSV(r)))
      case Some(None) => f(DejavuEventItem(writetoCSV(DejavuPrinter.DUMMY)))
      case None => ()
    }
    rec = None
  }
}

object DejavuPrinter{
  val DUMMY: Record = EventRecord(0,"DummyEventForTimeSkipping",IndexedSeq.empty)
}

class KeyedDejavuPrinter[K] extends Processor[(K, Record), DejavuRequest] with Serializable {
  private val internalPrinter = new DejavuPrinter

  @transient @volatile private var numberOfEvents: Long = 0

  override type State = DejavuPrinter#State

  override def getState: DejavuPrinter#State = internalPrinter.getState

  override def restoreState(state: Option[DejavuPrinter#State]): Unit = {
    internalPrinter.restoreState(state)
    numberOfEvents = 0
  }

  override def process(in: (K, Record), f: DejavuRequest => Unit): Unit = {
    if (!in._2.isEndMarker)
      numberOfEvents += 1
    internalPrinter.process(in._2, f)
  }

  override def terminate(f: DejavuRequest => Unit): Unit = internalPrinter.terminate(f)

  // TODO(JS): This doesn't really belong here.
  override def getCustomCounter: Long = numberOfEvents
}

object DejavuFormat extends TraceFormat {
  override def createParser(): DejavuParser with Serializable = new DejavuParser()
}

