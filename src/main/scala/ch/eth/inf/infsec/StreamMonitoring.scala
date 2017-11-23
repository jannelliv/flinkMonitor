package ch.eth.inf.infsec

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import shapeless.{Fin, Nat, Sized}


object Types{
  //Input types
  type Timestamp = Int
  abstract class Domain
  case class SInteger(v:Int) extends Domain
  case class SString(v:String) extends Domain
  case class SFloat(v:Float) extends Domain

  //type Relation = Set[Sized[Seq[Domain],Nat]]
  type Relation = Set[Seq[Domain]]

  type Event = (Timestamp,Set[Relation])
}


object StreamMonitoring {
  import Types._


  def parseLine(str: String):Event = ???
  //def monpoly(ev:) = ???

  def init(params:ParameterTool):DataStream[String] = {
    val hostName = params.get("hostname","127.0.0.1")
    val port = params.getInt("port",8000)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.socketTextStream(hostName, port)
  }

  def main(args: Array[String]) {

    //TODO: arg validation
    val params = ParameterTool.fromArgs(args)
    val processors = params.getInt("processors",1)

    val textStream = init(params)
    //    val textStream = FlinkAdapter.init(args)

    implicit val typeInfo = TypeInformation.of(classOf[Event])
    val parsedTrace:DataStream[(Timestamp,Set[Relation])] = textStream.map(parseLine _)
    val slicedTrace = Slicer(processors).slice(parsedTrace).keyBy(0)

    //val verdicts = slicedTrace.reduce(monpoly)





    //Type issue example with sized
//    val r:Relation = Set(Sized(SInteger(1),SString("a")),
//                         Sized(SInteger(2),SString("b")))
//    val e:Event = (4,Set(r))




  }

}


class Slicer(N:Int){
  import Types._

  //def slice(input:GenericStream[Event]) = input.flatMap[(Fin[N],Event)](slice)
  //def algorithm: Event => TraversableOnce[(Fin[N],Event)] = ???

  implicit val typeInfo = TypeInformation.of(classOf[(Set[Int],Event)])

  def slice(input:DataStream[Event]) = input.flatMap[(Set[Int],Event)](algorithm)
  def algorithm: Event => TraversableOnce[(Set[Int],Event)] = ???

}
object Slicer{
  def apply(N:Int): Slicer = new Slicer(N)

}


//trait GenericStream[T] {
//  def map[P:TypeInformation](f:T => P):GenericStream[P]
//  def flatMap[P:TypeInformation](f:T => TraversableOnce[P]):GenericStream[P]
//  def keyBy[K:TypeInformation](fun: T => K):GenericKStream[K,T]
//}
//
//trait GenericKStream[K,T]{
//  def reduce[P:TypeInformation](fun: (T,T) => T):GenericStream[T]
//}


