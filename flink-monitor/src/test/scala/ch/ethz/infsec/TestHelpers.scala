package ch.ethz.infsec

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

object TestHelpers {
  def flatMapAll[IN, OUT](f: FlatMapFunction[IN, OUT], values: Iterable[IN]): Seq[OUT] = {
    val output = new ArrayBuffer[OUT]()
    val collector = new Collector[OUT] {
      override def collect(record: OUT): Unit = output += record

      override def close(): Unit = ()
    }
    for (value <- values)
      f.flatMap(value, collector)
    output
  }
}
