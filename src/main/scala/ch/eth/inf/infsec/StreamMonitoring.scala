package ch.eth.inf.infsec

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import shapeless.{Fin, Nat, Sized}


object StreamMonitoring {
  var hostName:String=""
  var port:Int=0
  var processors:Int=0

  def init(params:ParameterTool) {
    hostName = params.get("hostname","127.0.0.1")
    port = params.getInt("port",9000)
    processors = params.getInt("processors",1)
  }

  def main(args: Array[String]) {

    //TODO: arg validation
    val params = ParameterTool.fromArgs(args)

    init(params)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textStream = env.socketTextStream(hostName, port)

    implicit val type1 = TypeInformation.of(classOf[Option[Event]])
    implicit val type2 = TypeInformation.of(classOf[Event])
    val parsedTrace:DataStream[Event] = textStream.map(parseLine _).filter(_.isDefined).map(_.get)

    parsedTrace.print.setParallelism(1)



    env.execute("Parallel Online Monitor")
  }

}


//val verdicts = slicedTrace.reduce(monpoly)

//Type issue example with sized
//    val r:Relation = Set(Sized(SInteger(1),SString("a")),
//                         Sized(SInteger(2),SString("b")))
//    val e:Event = (4,Set(r))


//trait GenericStream[T] {
//  def map[P:TypeInformation](f:T => P):GenericStream[P]
//  def flatMap[P:TypeInformation](f:T => TraversableOnce[P]):GenericStream[P]
//  def keyBy[K:TypeInformation](fun: T => K):GenericKStream[K,T]
//}
//
//trait GenericKStream[K,T]{
//  def reduce[P:TypeInformation](fun: (T,T) => T):GenericStream[T]
//}


