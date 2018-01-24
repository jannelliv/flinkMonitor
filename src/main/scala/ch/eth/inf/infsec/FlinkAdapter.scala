package ch.eth.inf.infsec

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.api.java.utils.ParameterTool


object FlinkAdapter {


  val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  def init(hostName:String, port:Int):Stream[String] = {
    new FlinkStream[String](env.socketTextStream(hostName, port))
  }
  def execute(name:String): Unit ={
    env.execute(name)
  }

}

class FlinkTypeInfo[A](val ti:TypeInformation[A]) extends TypeInfo[A]{}

//TODO: wrapped is temporary non-private
class FlinkStream[T]( val wrapped:DataStream[T]) extends Stream[T] {
  override type Self[A] = FlinkStream[A]

  override def map[U](f: T => U)(implicit  ev:TypeInfo[U]): Self[U] = {
    implicit val t = ev.asInstanceOf[FlinkTypeInfo[U]].ti;
    new Self[U](wrapped.map(f))}
  override def flatMap[U](f: T => TraversableOnce[U])(implicit  ev:TypeInfo[U]): Self[U] = {
    implicit val t = ev.asInstanceOf[FlinkTypeInfo[U]].ti;
    new Self[U](wrapped.flatMap(f))}
  override def filter(f: T => Boolean): Self[T] = new Self[T](wrapped.filter(f))

  override def print: Self[T] = {wrapped.print().setParallelism(1); this}

  override def partition[U](p: Criteria[U], i: Int): Stream[T] = ???
}

class FlinkPartitioner[T](private val wrapped: org.apache.flink.api.common.functions.Partitioner[T]) extends Criteria[T]{
  def partition(key: T, n: Int): Int = wrapped.partition(key,n)
}