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
//class FlinkStream[T](private val wrapped:DataStream[T]) extends Stream[T] {
//  override type Self[A] = DataStream[A]
//
//  override def map[U](f: T => U): Self[U] = new Self[U](wrapped.map(f))
//  override def flatMap[U](f: T => TraversableOnce[U]): Self[U] = new Self[U](wrapped.flatMap(f))
//}
//
//class FlinkKStream[K,T](private val wrapped:KeyedStream[T,K]) extends GenericKStream[K,T]{
//  override def reduce[P:TypeInformation](f: (T,T) => T):GenericStream[T] = new FlinkStream[T](wrapped.reduce(f))
//}