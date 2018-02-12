package ch.eth.inf.infsec.analysis

import ch.eth.inf.infsec.trace.{Event, Tuple}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.immutable

case class Rates(events: Long, tuples: Long) {
  def +(that: Rates): Rates = Rates(events + that.events, tuples + that.tuples)
}

class AggregateRatesFunction[K] extends AggregateFunction[(K, Iterable[Tuple]), Rates, Rates] {
  override def createAccumulator(): Rates = Rates(0, 0)

  override def add(in: (K, Iterable[Tuple]), acc: Rates): Rates = acc + Rates(1, in._2.size)

  override def getResult(acc: Rates): Rates = acc

  override def merge(acc1: Rates, acc2: Rates): Rates = acc1 + acc2
}

// TODO(JS): Implement proper sketching
case class Statistics(rates: Rates, counts: Array[immutable.HashMap[Any, Int]]) {
  def heavyHitters(degree: Int): IndexedSeq[Set[Any]] =
    counts.map(_.filter { case (_, c) => c >= rates.tuples / degree }.keySet)
}

class AggregateStatisticsFunction[K] extends AggregateFunction[(K, Iterable[Tuple]), Statistics, Statistics] {
  override def createAccumulator(): Statistics = Statistics(Rates(0, 0), Array())

  private def mergeCount(a: (Any, Int), b: (Any, Int)): (Any, Int) = (a._1, a._2 + b._2)

  override def add(in: (K, Iterable[Tuple]), acc: Statistics): Statistics =
    Statistics(
      Rates(1, in._2.size) + acc.rates,
      acc.counts.zipAll(in._2.transpose, immutable.HashMap.empty, immutable.HashMap.empty)
        .map { case (counts1, column) =>
          val counts2: immutable.HashMap[Any, Int] =
            column.groupBy(identity).map { case (k, v) => (k, v.size) }(collection.breakOut)
          counts1.merged(counts2)(mergeCount)
        }
    )

  override def getResult(acc: Statistics): Statistics = acc

  override def merge(acc1: Statistics, acc2: Statistics): Statistics =
    Statistics(
      acc1.rates + acc2.rates,
      acc1.counts.zipAll(acc2.counts, immutable.HashMap.empty, immutable.HashMap.empty)
        .map { case (counts1, counts2: immutable.HashMap[Any, Int]) => counts1.merged(counts2)(mergeCount) }
    )
}

class LabelWindowFunction[T, K] extends ProcessWindowFunction[T, (Long, K, T), K, TimeWindow] {
  override def process(
    key: K,
    context: Context,
    elements: Iterable[T],
    out: Collector[(Long, K, T)])
  {
    val aggregate = elements.head
    out.collect((context.window.getStart, key, aggregate))
  }
}

object TraceStatistics {

  def analyzeRelations(
      events: DataStream[Event],
      windowSize: Long,
      overlap: Long,
      degree: Int): DataStream[(Long, String, Statistics)] =
    events
      .flatMap(_.structure)
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSize / overlap)))
      .aggregate(new AggregateStatisticsFunction[String](), new LabelWindowFunction[Statistics, String]())

  def analyzeSlices(
      slices: DataStream[(Int, Event)],
      windowSize: Long,
      overlap: Long): DataStream[(Long, (Int, String), Rates)] =
    slices
      .flatMapWith { case (i, e) => e.structure.map { case (r, d) => ((i, r), d) } }
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSize / overlap)))
      .aggregate(new AggregateRatesFunction[(Int, String)](), new LabelWindowFunction[Rates, (Int, String)]())

}
