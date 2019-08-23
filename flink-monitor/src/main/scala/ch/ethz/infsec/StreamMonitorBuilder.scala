package ch.ethz.infsec

import ch.ethz.infsec.monitor.ExternalProcessOperator
import ch.ethz.infsec.slicer.HypercubeSlicer
import ch.ethz.infsec.tools.Rescaler.RescaleInitiator
import ch.ethz.infsec.trace.{TraceFormat, TraceMonitor}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.IdPartitioner
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SocketClientSink}
import org.apache.flink.streaming.api.functions.source.{FileProcessingMode, SocketTextStreamFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink

/**
 * Helper class that assembles the parallel monitor's data flow.
 */
class StreamMonitorBuilder(environment: StreamExecutionEnvironment) {
  def socketSource(host: String, port: Int): DataStream[String] = {
    val rawSource = new SocketTextStreamFunction(host, port, "\n", 0)
    LatencyTrackingExtensions.addSourceWithProvidedMarkers(environment, rawSource, "Socket source")
      .uid("socket-source")
  }

  def fileWatchSource(path: String): DataStream[String] = {
    environment.readFile(new TextInputFormat(new Path(path)), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
      .name("File watch source")
      .uid("file-watch-source")
  }

  def simpleFileSource(path: String): DataStream[String] = {
    environment.readTextFile(path)
      .name("Simple file source")
      .uid("simple-file-source")
  }

  def socketSink(verdicts: DataStream[String], host: String, port: Int): Unit = {
    val outVerdicts = verdicts.map(v => v + "\n")
      .setParallelism(1)
      .name("Add newline")
      .uid("add-newline")

    val sink = new SocketClientSink[String](host, port, new SimpleStringSchema(), 0)
    LatencyTrackingExtensions.addPreciseLatencyTrackingSink(outVerdicts, sink)
      .setParallelism(1)
      .name("Socket sink")
      .uid("socket-sink")
  }

  def fileSink(verdicts: DataStream[String], path: String): Unit = {
    LatencyTrackingExtensions.addPreciseLatencyTrackingSink(verdicts, new BucketingSink[String](path))
      .setParallelism(1)
      .name("File sink")
      .uid("file-sink")
  }

  def printSink(verdicts: DataStream[String]): Unit = {
    LatencyTrackingExtensions.addPreciseLatencyTrackingSink(verdicts, new PrintSinkFunction[String]())
      .setParallelism(1)
      .name("Print sink")
      .uid("print-sink")
  }

  def assemble(inputStream: DataStream[String],
               traceFormat: TraceFormat,
               slicer: HypercubeSlicer,
               monitorFactory: MonitorFactory,
               queueSize: Int): DataStream[String] = {

    val parser = new TraceMonitor(traceFormat.createParser(), new RescaleInitiator().rescale)
    val parsedTrace = inputStream.flatMap(new ProcessorFunction(parser))
      .name("Trace parser")
      .uid("trace-parser")
      .setMaxParallelism(1)
      .setParallelism(1)

    val slicedTrace = parsedTrace
      .flatMap(new ProcessorFunction(slicer))
      .name("Slicer")
      .uid("slicer")
      .setMaxParallelism(1)
      .setParallelism(1)
      .partitionCustom(new IdPartitioner, 0)

    val verdicts = ExternalProcessOperator.transform(slicer, slicedTrace, monitorFactory, queueSize)
      .setParallelism(slicer.degree)
      .setMaxParallelism(slicer.degree)
      .name("Monitor")
      .uid("monitor")

    verdicts
  }
}
