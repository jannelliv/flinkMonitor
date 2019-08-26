package ch.ethz.infsec
package trace

import ch.ethz.infsec.monitor._
import fastparse.WhitespaceApi
import fastparse.noApi._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

class DejavuVerdictFilter extends StatelessProcessor[String,String] with Serializable{

  override def process(in: String, f: String => Unit): Unit = {
    if (in!=null && in.length>DejavuProcess.VIOLATION_PREFIX.length && in.startsWith(DejavuProcess.VIOLATION_PREFIX)) {
      f(in.substring(DejavuProcess.VIOLATION_PREFIX.length-1, in.length - 1))
    }
  }

  override def terminate(f: String => Unit): Unit = ()
}

class DejavuPrinter[T] extends Processor[Either[Record,T], Either[DejavuRequest,T]] with Serializable {
  override type State = (Option[Option[Record]], Vector[T])
  protected var rec: Option[Option[Record]] = None
  protected var delayBuffer =  new ArrayBuffer[T]()
  /*
    State semantics:
    None = Empty trace
    Some(None) = Empty time point
    Some(Some(r)) = event r

   */

  override def getState: State = (rec,delayBuffer.toVector)

  override def restoreState(state: Option[State]): Unit = state  match {
    case Some((r,db)) => rec = r; delayBuffer ++=db
    case None => rec = None
  }

  // DejaVu ignores commands
  def processCommand(record: Record, f: Either[DejavuRequest,T] => Unit): Unit = ()

  def processEvent(record: Record, f: Either[DejavuRequest,T] => Unit): Unit = {
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


  override def process(record: Either[Record,T], f: Either[DejavuRequest,T] => Unit) {
    record match {
      case Left(rec@CommandRecord(_, _)) => processCommand(rec, f)
      case Left(rec@EventRecord(_, _, _)) => processEvent(rec, f)
      case Right(value) if rec.isEmpty => f(Right(value))
      case Right(value) => delayBuffer += value
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
    str.append('\n')
    str.toString()
  }


  override def terminate(f: Either[DejavuRequest,T] => Unit): Unit = {
    rec match {
      case Some(Some(r)) => f(Left(DejavuEventItem(writetoCSV(r))))
      case Some(None) => f(Left(DejavuEventItem(writetoCSV(DejavuPrinter.DUMMY))))
      case None => ()
    }
    rec = None
    for (i <- delayBuffer){
      f(Right(i))
    }
    delayBuffer.clear()
  }
}

object DejavuPrinter{
  val DUMMY: Record = EventRecord(0,"DummyEventForTimeSkipping",IndexedSeq.empty)
}

class KeyedDejavuPrinter[K,T] extends Processor[Either[(K, Record),T], Either[DejavuRequest,T]] with Serializable {
  private val internalPrinter = new DejavuPrinter[T]

  @transient @volatile private var numberOfEvents: Long = 0

  override type State = DejavuPrinter[T]#State

  override def getState: DejavuPrinter[T]#State = internalPrinter.getState

  override def restoreState(state: Option[DejavuPrinter[T]#State]): Unit = {
    internalPrinter.restoreState(state)
    numberOfEvents = 0
  }

  override def process(in: Either[(K, Record),T], f: Either[DejavuRequest,T] => Unit): Unit = {
    in match {
      case Left((_,rec)) => {
        if (!rec.isEndMarker)
          numberOfEvents += 1
        internalPrinter.process(Left(rec), f)
      }
      case Right(r) => internalPrinter.process(Right(r),f)
    }

  }

  override def terminate(f: Either[DejavuRequest,T] => Unit): Unit = internalPrinter.terminate(f)

  // TODO(JS): This doesn't really belong here.
  override def getCustomCounter: Long = numberOfEvents
}

object DejavuFormat extends TraceFormat {
  override def createParser(): DejavuParser with Serializable = new DejavuParser()
}

