package ch.eth.inf.infsec

import org.apache.flink.api.java.utils.ParameterTool
import shapeless.{Fin, Nat, Sized}

import scala.reflect.api.TypeTags


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

    val textStream = FlinkAdapter.init(hostName,port)
    //FlinkAdapter.registerTypes(classOf[Option[Event]],classOf[Event])

    val parsedTrace:Stream[Event] = textStream.map(parseLine _).filter(_.isDefined).map(_.get)

  val a:TypeTags
    parsedTrace.print



    FlinkAdapter.execute("Parallel Online Monitor")
  }

}


//val verdicts = slicedTrace.reduce(monpoly)

//Type issue example with sized
//    val r:Relation = Set(Sized(SInteger(1),SString("a")),
//                         Sized(SInteger(2),SString("b")))
//    val e:Event = (4,Set(r))



