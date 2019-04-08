package ch.ethz.infsec

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.InputTypeConfigurable
import org.apache.flink.api.scala.ClosureCleaner
import org.apache.flink.metrics.Gauge
import org.apache.flink.runtime.jobgraph.OperatorID
import org.apache.flink.streaming.api.datastream.{DataStreamSink, DataStreamSource}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.graph.StreamConfig
import org.apache.flink.streaming.api.operators.{Output, StreamOperator, StreamSink, StreamSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.{LatencyMarker, StreamRecord}
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer
import org.apache.flink.streaming.runtime.tasks.StreamTask
import org.apache.flink.util.OutputTag

private class ProvidedLatencyOutputDecorator(
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
    if (env.getConfig.isClosureCleanerEnabled)
      ClosureCleaner.clean(sourceFunction, checkSerializable = true)
    else
      ClosureCleaner.ensureSerializable(sourceFunction)
    val typeInfo = implicitly[TypeInformation[String]]

    env.getJavaEnv.clean(sourceFunction)

    val sourceOperator = new ProvidedLatencyStreamSource(sourceFunction)
    val sourceStream = new DataStreamSource(env.getJavaEnv, typeInfo, sourceOperator, false, sourceName)
    new DataStream[String](sourceStream.returns(typeInfo))
  }

  def addPreciseLatencyTrackingSink[T](stream: DataStream[T], sinkFunction: SinkFunction[T]): DataStreamSink[T] = {
    val env = stream.executionEnvironment
    if (env.getConfig.isClosureCleanerEnabled)
      ClosureCleaner.clean(sinkFunction, checkSerializable = true)
    else
      ClosureCleaner.ensureSerializable(sinkFunction)

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
}
