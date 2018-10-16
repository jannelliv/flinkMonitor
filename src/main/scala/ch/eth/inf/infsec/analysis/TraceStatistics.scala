package ch.eth.inf.infsec.analysis

import ch.eth.inf.infsec.trace.{Domain, Record}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

class CountFunction[T] extends AggregateFunction[T, Long, Long] {
  override def add(value: T, acc: Long): Long = acc + 1

  override def createAccumulator(): Long = 0

  override def getResult(acc: Long): Long = acc

  override def merge(a: Long, b: Long): Long = a + b
}

class LabelWindowFunction[T, K] extends ProcessWindowFunction[T, (Long, K, T), K, TimeWindow] {
  override def process(
    key: K,
    context: Context,
    elements: Iterable[T],
    out: Collector[(Long, K, T)]) {
    val aggregate = elements.head
    out.collect((context.window.getStart, key, aggregate))
  }
}

case class Statistics(events: Long, heavyCounts: Array[Array[(Domain, Int)]]) {
  def heavyHitters: IndexedSeq[Set[Domain]] = heavyCounts.map(_.view.map(_._1).toSet)
}

// TODO(JS): Implement proper sketching/sampling
class StatisticsFunction[K](degree: Int, minThreshold: Int)
    extends ProcessWindowFunction[Record, (Long, K, Statistics), K, TimeWindow] {

  override def process(
    key: K,
    context: Context,
    elements: Iterable[Record],
    out: Collector[(Long, K, Statistics)]): Unit = {

    val events = elements.size
    val attributes = elements.headOption.map(_.data.length).getOrElse(0)
    // We divide the threshold by 2 to account for our use of tumbling windows.
    val threshold = (events.toDouble / (2.0 * degree.toDouble)).ceil.toInt.max(minThreshold)

    val counts = Array.fill(attributes)(new mutable.HashMap[Domain, Int].withDefaultValue(0))
    for (record <- elements)
      for ((value, i) <- record.data.zipWithIndex)
        counts(i)(value) += 1

    for (i <- 0 until attributes)
      for ((value, count) <- counts(i) if count < threshold)
        counts(i).remove(value)

    out.collect((context.window.getStart, key, Statistics(events, counts.map(_.toArray))))
  }
}

object TraceStatistics {

  def analyzeRelationFrequencies(events: DataStream[Record], windowSize: Long): DataStream[(Long, String, Long)] =
    events
      .keyBy(_.label)
      .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
      .aggregate(new CountFunction[Record](), new LabelWindowFunction[Long, String]())

  def analyzeRelations(
      events: DataStream[Record],
      windowSize: Long,
      degree: Int,
      minThreshold: Int): DataStream[(Long, String, Statistics)] =
    events
      .keyBy(_.label)
      .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
      .process(new StatisticsFunction[String](degree, minThreshold))

  def analyzeSlices(slices: DataStream[(Int, Record)], windowSize: Long): DataStream[(Long, (Int, String), Long)] =
    slices
      .keyBy(r => (r._1, r._2.label))
      .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
      .aggregate(new CountFunction[(Int, Record)](), new LabelWindowFunction[Long, (Int, String)]())

}
