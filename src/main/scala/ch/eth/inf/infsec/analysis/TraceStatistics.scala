package ch.eth.inf.infsec.analysis

import ch.eth.inf.infsec.trace.{Domain, Record, Tuple}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.immutable

class CountFunction[T] extends AggregateFunction[T, Long, Long] {
  override def add(value: T, acc: Long): Long = acc + 1

  override def createAccumulator(): Long = 0

  override def getResult(acc: Long): Long = acc

  override def merge(a: Long, b: Long): Long = a + b
}

// TODO(JS): Implement proper sketching
case class Statistics(records: Long, counts: Array[immutable.HashMap[Domain, Int]]) {
  def heavyHitters(degree: Int): IndexedSeq[Set[Domain]] =
    counts.map(_.filter { case (_, c) => c >= records.toDouble / degree }.keySet)

  def withTuple(data: Tuple): Statistics =
    Statistics(
      records + 1,
      counts.zipAll(data, immutable.HashMap.empty, null)
        .map { case (cs, x) => if (x == null) cs else cs.updated(x, cs.getOrElse(x, 0) + 1) }
    )
}

class AggregateStatisticsFunction extends AggregateFunction[Record, Statistics, Statistics] {
  override def createAccumulator(): Statistics = Statistics(0, Array())

  private def mergeCount(a: (Domain, Int), b: (Domain, Int)): (Domain, Int) = (a._1, a._2 + b._2)

  override def add(in: Record, acc: Statistics): Statistics = acc.withTuple(in.data)

  override def getResult(acc: Statistics): Statistics = acc

  override def merge(acc1: Statistics, acc2: Statistics): Statistics =
    Statistics(
      acc1.records + acc2.records,
      acc1.counts.zipAll(acc2.counts, immutable.HashMap.empty, immutable.HashMap.empty)
        .map { case (counts1, counts2: immutable.HashMap[Domain, Int]) => counts1.merged(counts2)(mergeCount) }
    )
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

object TraceStatistics {

  def analyzeRelationFrequencies(
      events: DataStream[Record],
      windowSize: Long,
      overlap: Long): DataStream[(Long, String, Long)] =
    events
      .keyBy(_.label)
      .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSize / overlap)))
      .aggregate(new CountFunction[Record](), new LabelWindowFunction[Long, String]())

  def analyzeRelations(
      events: DataStream[Record],
      windowSize: Long,
      overlap: Long,
      degree: Int): DataStream[(Long, String, Statistics)] =
    events
      .keyBy(_.label)
      .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSize / overlap)))
      .aggregate(new AggregateStatisticsFunction(), new LabelWindowFunction[Statistics, String]())

  def analyzeSlices(
      slices: DataStream[(Int, Record)],
      windowSize: Long,
      overlap: Long): DataStream[(Long, (Int, String), Long)] =
    slices
      .keyBy(r => (r._1, r._2.label))
      .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSize / overlap)))
      .aggregate(new CountFunction[(Int, Record)](), new LabelWindowFunction[Long, (Int, String)]())

}
