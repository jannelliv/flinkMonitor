package ch.eth.inf.infsec

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.api.java.utils.ParameterTool


//object FlinkAdapter {
//
//  def init(args:Array[String]):GenericStream[String] = {
//    val params = ParameterTool.fromArgs(args)
//    val hostName = params.get("hostname","127.0.0.1")
//    val port = params.getInt("port",8000)
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    new FlinkStream[String](env.socketTextStream(hostName, port))
//  }
//
//}
//
//
//
//class FlinkStream[T](private val wrapped:DataStream[T]) extends GenericStream[T] {
//  override def map[P:TypeInformation](f:T => P):GenericStream[P] = new FlinkStream[P](wrapped.map(f))
//  override def flatMap[P:TypeInformation](f:T => TraversableOnce[P]):GenericStream[P] = new FlinkStream[P](wrapped.flatMap(f))
//  override def keyBy[K:TypeInformation](f: T => K):GenericKStream[K,T] = new FlinkKStream[K,T](wrapped.keyBy(f))
//}
//
//class FlinkKStream[K,T](private val wrapped:KeyedStream[T,K]) extends GenericKStream[K,T]{
//  override def reduce[P:TypeInformation](f: (T,T) => T):GenericStream[T] = new FlinkStream[T](wrapped.reduce(f))
//}