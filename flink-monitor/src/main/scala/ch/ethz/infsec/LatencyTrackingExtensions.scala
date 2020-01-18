package ch.ethz.infsec

import java.util.stream.Collector

import ch.ethz.infsec.monitor.Fact
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Gauge
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator}
import org.apache.flink.util

import scala.util.control.Breaks.{break, breakable}
import scala.collection.mutable

class LatencyProcessingFunction extends RichFlatMapFunction[Fact, Fact] {
  @transient private var delays: Array[(Long, Int)] = _
  @transient private var currentDelayIndex: Int = 0
  @transient private var delaySum: Long = 0
  @transient private var delaySamples: Int = 0
  @transient private var maxDelay: Int = 0
  @transient private var latencyMarkerMap: mutable.LongMap[(Int, Int, Long)] = _
  @transient private var currIdx: Long = _

  private val numExpectedMarkers = StreamMonitoring.inputParallelism * StreamMonitoring.processors

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    this.delays = Array.fill(20)((0, 0))
    this.currentDelayIndex = 0
    this.delaySum = 0
    this.delaySamples = 0
    this.maxDelay = 0
    this.latencyMarkerMap = new mutable.LongMap[(Int, Int, Long)]()
    this.currIdx = 0

    val metricGroup = getRuntimeContext.getMetricGroup.addGroup("latency")
    metricGroup.gauge[Int, Gauge[Int]]("peak", new Gauge[Int] {
      override def getValue: Int = {
        val earliest = System.currentTimeMillis() - 1000
        delays.filter(_._1 > earliest).foldLeft(0)(_ max _._2)
      }
    })
    metricGroup.gauge[Int, Gauge[Int]]("max", new Gauge[Int] {
      override def getValue: Int = maxDelay
    })
    metricGroup.gauge[Int, Gauge[Int]]("average", new Gauge[Int] {
      override def getValue: Int = (delaySum.toDouble / delaySamples.toDouble).round.toInt
    })
  }

  def updateLatency(): Unit = {
    var i = currIdx;
    breakable {
      while (true) {
        latencyMarkerMap.get(i) match {
          case Some((count, delay, time)) =>
            if (count == numExpectedMarkers) {
              //println(s"lol got all expected latency markers with idx $i, delay is $delay")
              currentDelayIndex += 1
              if (currentDelayIndex >= delays.length)
                currentDelayIndex = 0
              delays(currentDelayIndex) = (time, delay)
              delaySum += delay
              delaySamples += 1
              maxDelay = maxDelay.max(delay)
              //println(s"LOL max delay now is $maxDelay")
            } else {
              break()
            }
          case None => break()
        }
        i += 1
      }
    }
    currIdx = i
  }

  override def flatMap(in: Fact, collector: util.Collector[Fact]): Unit = {
    //println("LOL latency tracking: " + in)
    if (in.isMeta && in.getName == "LATENCY") {
      //println("lol got latency fact: " + in)
      val idx = java.lang.Long.parseLong(in.getArgument(0).asInstanceOf[java.lang.String])
      if (idx < currIdx) {
        throw new RuntimeException(s"INVARIANT: expected latency meta fact with idx >= ${currIdx}, has idx ${idx}")
      }
      val markedTime = java.lang.Long.parseLong(in.getArgument(1).asInstanceOf[java.lang.String])
      val now = System.currentTimeMillis()
      val inDelay = (now - markedTime).toInt
      val count = latencyMarkerMap.get(idx) match {
        case Some((count, delay, time)) =>
          val (delayNew, timeNew) = if (delay >= inDelay) (delay, time) else (inDelay, now)
          latencyMarkerMap += (idx -> (count+1, delayNew, timeNew))
          count+1
        case None =>
          latencyMarkerMap += (idx -> (1, inDelay, now))
          1
      }
      if (count == numExpectedMarkers && idx == currIdx) {
        updateLatency()
      } else if (count > numExpectedMarkers) {
        throw new RuntimeException(s"INVARIANT: too many latency meta facts received, expected ${numExpectedMarkers} got ${count}")
      }
    } else {
      collector.collect(in)
    }
  }
}

/*private class ProvidedLatencyOutputDecorator(
    output: Output[StreamRecord[String]],
    operatorID: OperatorID,
    subtaskIndex: Int) extends Output[StreamRecord[String]] {

  private val TIMESTAMP_PREFIX = "###"

  override def emitWatermark(watermark: Watermark): Unit = output.emitWatermark(watermark)

  override def collect[X](outputTag: OutputTag[X], streamRecord: StreamRecord[X]): Unit =
    output.collect(outputTag, streamRecord)

  override def emitLatencyMarker(latencyMarker: LatencyMarker): Unit = ()

  override def collect(streamRecord: StreamRecord[String]): Unit = {
    val value = streamRecord.getValue
    if (value.startsWith(TIMESTAMP_PREFIX)) {
      val time = value.substring(TIMESTAMP_PREFIX.length).trim.toLong
      println("Input latency "  + time + " at " + System.currentTimeMillis() + " for subtask " + subtaskIndex)
      output.emitLatencyMarker(new LatencyMarker(time, operatorID, subtaskIndex))
    } else
      output.collect(streamRecord)
  }

  override def close(): Unit = output.close()
}

private class ProvidedLatencyStreamSource[SRC <: SourceFunction[String]](sourceFunction: SRC)
    extends StreamSource[String, SRC](sourceFunction) {

  override def run(
      lockingObject: scala.Any,
      streamStatusMaintainer: StreamStatusMaintainer,
      collector: Output[StreamRecord[String]]): Unit =
    super.run(
      lockingObject, streamStatusMaintainer, new ProvidedLatencyOutputDecorator(
        collector, getOperatorID, getRuntimeContext.getIndexOfThisSubtask))

}

private class PreciseLatencyTackingStreamSink[IN](sinkFunction: SinkFunction[IN]) extends StreamSink[IN](sinkFunction) {
  @transient private var delays: Array[(Long, Int)] = _
  @transient private var currentDelayIndex: Int = 0
  @transient private var delaySum: Long = 0
  @transient private var delaySamples: Int = 0
  @transient private var maxDelay: Int = 0

  override def reportOrForwardLatencyMarker(marker: LatencyMarker): Unit = {
    val now = System.currentTimeMillis()
    val delay = (now - marker.getMarkedTime).toInt

    currentDelayIndex += 1
    if (currentDelayIndex >= delays.length)
        currentDelayIndex = 0
    delays(currentDelayIndex) = (now, delay)
    delaySum += delay
    delaySamples += 1
    maxDelay = maxDelay.max(delay)
//    println("Output latency: delay " + delay + " with time "+ marker.getMarkedTime + " at " + System.currentTimeMillis())
    super.reportOrForwardLatencyMarker(marker)
  }

  override def setup(
      containingTask: StreamTask[_, _],
      config: StreamConfig,
      output: Output[StreamRecord[AnyRef]]): Unit = {
    super.setup(containingTask, config, output)

    delays = Array.fill(20)((0, 0))
    currentDelayIndex = 0
    delaySum = 0
    delaySamples = 0
    maxDelay = 0

    val metricGroup = getMetricGroup.addGroup("latency")
    metricGroup.gauge[Int, Gauge[Int]]("peak", new Gauge[Int] {
      override def getValue: Int = {
        val earliest = System.currentTimeMillis() - 1000
        delays.filter(_._1 > earliest).foldLeft(0)(_ max _._2)
      }
    })
    metricGroup.gauge[Int, Gauge[Int]]("max", new Gauge[Int] {
      override def getValue: Int = maxDelay
    })
    metricGroup.gauge[Int, Gauge[Int]]("average", new Gauge[Int] {
      override def getValue: Int = (delaySum.toDouble / delaySamples.toDouble).round.toInt
    })
  }
}

object LatencyTrackingExtensions {
  def addSourceWithProvidedMarkers(
      env: StreamExecutionEnvironment,
      sourceFunction: SourceFunction[String],
      sourceName: String): DataStream[String] = {
    // NOTE(JS): ClosureCleaner#ensureSerializable is no longer accessible
    //if (env.getConfig.isClosureCleanerEnabled)
      ClosureCleaner.clean(sourceFunction, checkSerializable = true)
    //else
    //  ClosureCleaner.ensureSerializable(sourceFunction)
    val typeInfo = implicitly[TypeInformation[String]]

    env.getJavaEnv.clean(sourceFunction)

    val sourceOperator = new ProvidedLatencyStreamSource(sourceFunction)
    val sourceStream = new DataStreamSource(env.getJavaEnv, typeInfo, sourceOperator, true, sourceName)
    new DataStream[String](sourceStream.returns(typeInfo))
  }

  def addPreciseLatencyTrackingSink[T](stream: DataStream[T], sinkFunction: SinkFunction[T]): DataStreamSink[T] = {
    val env = stream.executionEnvironment
    // NOTE(JS): ClosureCleaner#ensureSerializable is no longer accessible
    //if (env.getConfig.isClosureCleanerEnabled)
      ClosureCleaner.clean(sinkFunction, checkSerializable = true)
    //else
    //  ClosureCleaner.ensureSerializable(sinkFunction)

    env.getJavaEnv.clean(sinkFunction)

    sinkFunction match {
      case configurable: InputTypeConfigurable =>
        configurable.setInputType(stream.dataType, stream.executionConfig)
      case _ =>
    }

    val sinkOperator = new PreciseLatencyTackingStreamSink[T](sinkFunction)
    val sink = new DataStreamSink[T](stream.javaStream, sinkOperator) { }

    env.getJavaEnv.addOperator(sink.getTransformation)
    sink
  }
}*/
