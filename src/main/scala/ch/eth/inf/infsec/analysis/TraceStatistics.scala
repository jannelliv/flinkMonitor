package ch.eth.inf.infsec.analysis

import ch.eth.inf.infsec.trace.{Event, Tuple}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.immutable

case class Rates(events: Double, tuples: Double)

class AggregateRatesFunction[K](val windowSize: Long)
  extends AggregateFunction[(K, Iterable[Tuple]), (Option[K], Long, Long), (K, Rates)] {

  type Acc = (Option[K], Long, Long)

  override def createAccumulator(): Acc = (None, 0, 0)

  override def add(in: (K, Iterable[Tuple]), acc: Acc): Acc = (Some(in._1), acc._2 + 1, acc._3 + in._2.size)

  override def getResult(acc: Acc): (K, Rates) =
    (acc._1.get, Rates(acc._2.toDouble / windowSize, acc._3.toDouble / windowSize))

  override def merge(acc1: Acc, acc2: Acc): Acc = (acc1._1.orElse(acc2._1), acc1._2 + acc2._2, acc1._3 + acc2._3)
}

case class Statistics(relation: String, rates: Rates, heavyHitters: IndexedSeq[Set[Any]])

// TODO(JS): Implement proper sketching
case class StatisticsAccumulator(rateAcc: (Option[String], Long, Long), sketch: Array[immutable.HashMap[Any, Int]])

class AggregateStatisticsFunction(val windowSize: Long, val degree: Int)
  extends AggregateFunction[(String, Iterable[Tuple]), StatisticsAccumulator, Statistics] {

  protected val aggregateRates: AggregateRatesFunction[String] = new AggregateRatesFunction[String](windowSize)

  override def createAccumulator(): StatisticsAccumulator =
    StatisticsAccumulator(aggregateRates.createAccumulator(), Array())

  private def mergeCount(a: (Any, Int), b: (Any, Int)): (Any, Int) = (a._1, a._2 + b._2)

  override def add(in: (String, Iterable[Tuple]), acc: StatisticsAccumulator): StatisticsAccumulator =
    StatisticsAccumulator(
      aggregateRates.add(in, acc.rateAcc),
      acc.sketch.zipAll(in._2.transpose, immutable.HashMap.empty, immutable.HashMap.empty)
        .map { case (counts1, column) =>
          val counts2: immutable.HashMap[Any, Int] =
            column.groupBy(identity).map { case (k, v) => (k, v.size) }(collection.breakOut)
          counts1.merged(counts2)(mergeCount)
        }
    )

  override def getResult(acc: StatisticsAccumulator): Statistics = {
    val (relation, rates) = aggregateRates.getResult(acc.rateAcc)
    val heavyHitters = acc.sketch.map(_.filter { case (_, c) => c.toDouble / windowSize >= rates.tuples / degree }.keySet)
    Statistics(relation, rates, heavyHitters)
  }

  override def merge(acc1: StatisticsAccumulator, acc2: StatisticsAccumulator): StatisticsAccumulator =
    StatisticsAccumulator(
      aggregateRates.merge(acc1.rateAcc, acc2.rateAcc),
      acc1.sketch.zipAll(acc2.sketch, immutable.HashMap.empty, immutable.HashMap.empty)
        .map { case (counts1, counts2: immutable.HashMap[Any, Int]) => counts1.merged(counts2)(mergeCount) }
    )
}

object TraceStatistics {

  def analyzeRelations(
      events: DataStream[Event],
      windowSize: Long,
      overlap: Long,
      degree: Int): DataStream[Statistics] =
    events
      .flatMap(_.structure)
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSize / overlap)))
      .aggregate(new AggregateStatisticsFunction(windowSize, degree))

  def analyzeSlices(
      slices: DataStream[(Int, Event)],
      windowSize: Long,
      overlap: Long): DataStream[((Int, String), Rates)] =
    slices
      .flatMapWith { case (i, e) => e.structure.map { case (r, d) => ((i, r), d) } }
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSize / overlap)))
      .aggregate(new AggregateRatesFunction(windowSize))

}
