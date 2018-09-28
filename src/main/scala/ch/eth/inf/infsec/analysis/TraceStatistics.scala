package ch.eth.inf.infsec.analysis

import ch.eth.inf.infsec.trace.{Domain, Record, Tuple}
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

// TODO(JS): Implement proper sketching
case class Statistics(records: Long, counts: Map[List[Option[Domain]], Int]) {
  def heavyHitters(degree: Int, minThreshold: Int): IndexedSeq[Set[Domain]] = {
    // We divide the threshold by 2 to account for our use of tumbling windows.
    val threshold = (records.toDouble / (2.0 * degree.toDouble)).ceil.toInt.max(minThreshold)
    val acc = Vector.fill(counts.headOption.map(_._1.length).getOrElse(0))(new mutable.HashSet[Domain]())
    for ((key, count) <- counts if count >= threshold && key.count(_.isDefined) == 1) {
      val attribute = key.indexWhere(_.isDefined)
      acc(attribute) += key(attribute).get
    }
    acc.map(_.toSet)
  }

  def allHeavyHitters(degree: Int, minThreshold: Int): Map[List[Option[Domain]], Int] = {
    val threshold = (records.toDouble / (2.0 * degree.toDouble)).ceil.toInt.max(minThreshold)
    counts.filter{ case (_, count) => count >= threshold }
  }
}

class StatisticsFunction[K] extends ProcessWindowFunction[Record, (Long, K, Statistics), K, TimeWindow] {
  override def process(
    key: K,
    context: Context,
    elements: Iterable[Record],
    out: Collector[(Long, K, Statistics)]): Unit = {

    val acc = new mutable.HashMap[List[Option[Domain]], Int].withDefaultValue(0)
    def addValue(data: Tuple, index: Int, partialKey: List[Option[Domain]]): Unit =
      if (index < 0) {
        acc(partialKey) += 1
      } else {
        addValue(data, index - 1, None :: partialKey)
        addValue(data, index - 1, Some(data(index)) :: partialKey)
      }

    for (record <- elements)
      addValue(record.data, record.data.length - 1, Nil)

    out.collect((context.window.getStart, key, Statistics(elements.size, acc.toMap)))
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
      degree: Int): DataStream[(Long, String, Statistics)] =
    events
      .keyBy(_.label)
      .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
      .process(new StatisticsFunction[String]())

  def analyzeSlices(slices: DataStream[(Int, Record)], windowSize: Long): DataStream[(Long, (Int, String), Long)] =
    slices
      .keyBy(r => (r._1, r._2.label))
      .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
      .aggregate(new CountFunction[(Int, Record)](), new LabelWindowFunction[Long, (Int, String)]())

}
