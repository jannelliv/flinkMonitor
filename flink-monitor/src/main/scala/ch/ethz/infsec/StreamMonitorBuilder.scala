package ch.ethz.infsec

import ch.ethz.infsec.monitor.{ExternalProcess, ExternalProcessOperator, Fact}
import ch.ethz.infsec.slicer.{HypercubeSlicer, VerdictFilter}
import ch.ethz.infsec.tools.{MonitorKafkaConfig, ReorderFactsFunction, TestSimpleStringSchema}
import ch.ethz.infsec.trace.formatter.MonpolyVerdictFormatter
import ch.ethz.infsec.trace.parser.TraceParser
import ch.ethz.infsec.trace.{ParsingFunction, PrintingFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.IdPartitioner
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SocketClientSink}
import org.apache.flink.streaming.api.functions.source.{FileProcessingMode, SocketTextStreamFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

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

  def kafkaSource() : DataStream[String] = {
    val rawSource = new FlinkKafkaConsumer011[String](MonitorKafkaConfig.getTopic,
      new TestSimpleStringSchema,
      MonitorKafkaConfig.getKafkaProps)
    rawSource.setStartFromEarliest()
    environment.addSource(rawSource)
      .setParallelism(StreamMonitoring.inputParallelism)
      .setMaxParallelism(StreamMonitoring.inputParallelism)
      .name("Kafka source")
      .uid("kafka-source")
      .flatMap(s => {
        if (s.startsWith("EOF"))
          List()
        else
          List(s)
      })
      .setParallelism(StreamMonitoring.inputParallelism)
      .setMaxParallelism(StreamMonitoring.inputParallelism)
  }

  def simpleFileSource(path: String): DataStream[String] = {
    environment.readTextFile(path)
      .name("Simple file source")
      .uid("simple-file-source")
  }

  def socketSink(verdicts: DataStream[String], host: String, port: Int): Unit = {
    val sink = new SocketClientSink[String](host, port, new SimpleStringSchema(), 0)
    LatencyTrackingExtensions.addPreciseLatencyTrackingSink(verdicts, sink)
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
               traceFormat: TraceParser,
//               decider: Option[DeciderFlatMapSimple],
               slicer: HypercubeSlicer,
               monitorProcess: ExternalProcess[Fact, Fact],
               queueSize: Int): DataStream[String] = {

//    val parser = new TraceMonitor(traceFormat.createParser(), new RescaleInitiator().rescale)
    val parsedTrace = inputStream.flatMap(new ParsingFunction(traceFormat))
      .name("Trace parser")
      .uid("trace-parser")
      .setMaxParallelism(StreamMonitoring.inputParallelism)
      .setParallelism(StreamMonitoring.inputParallelism)

    /*
    val injectedTrace = parsedTrace.flatMap(new ProcessorFunction[Record,Record](new OutsideInfluence(true).asInstanceOf[Processor[Record,Record] with Serializable]))

    val observedTrace = decider match {
      case None => injectedTrace
      case Some(deciderFlatMap) =>
        //todo: proper arguments
        injectedTrace.flatMap(new ProcessorFunction[Record, Record](new AllState(deciderFlatMap)))
          .setMaxParallelism(1)
          .setParallelism(1)
          .name("Decider")
          .uid("decider")
    }
    */
    val slicedTrace = parsedTrace
      .flatMap(slicer)
      .setMaxParallelism(StreamMonitoring.inputParallelism)
      .setParallelism(StreamMonitoring.inputParallelism)
      .name("Slicer")
      .uid("slicer")

    val partitionedTraceWithoutId = slicedTrace
      .partitionCustom(new IdPartitioner, 0)
      .map(_._2)
      .setParallelism(slicer.degree)
      .setMaxParallelism(slicer.degree)
      .name("Remove slice ID and partition")
      .uid("remove-id")

    val reorderedTrace = partitionedTraceWithoutId
      .flatMap(new ReorderFactsFunction(slicer.degree))
      .setParallelism(slicer.degree)
      .setMaxParallelism(slicer.degree)

    // XXX(JS): Implement this comment here:
    // We do not send any commands to unused submonitors. In particular, we cannot use their state fragments
    // because the state is not synchronized with the global progress. Ideally, we would not even create
    // such submonitors.
    // TODO(JS): Check the splitting/merging logic in the case of unused submonitors.

    val rawVerdicts = ExternalProcessOperator.transform(reorderedTrace, monitorProcess, queueSize)
      .setParallelism(slicer.degree)
      .setMaxParallelism(slicer.degree)
      .name("Monitor")
      .uid("monitor")

    //verdictsAndOtherThings.flatMap(new ProcessorFunction(new KnowledgeExtract(slicer.degree)))

    val filteredVerdicts = rawVerdicts.filter(new VerdictFilter(slicer))
        .setParallelism(slicer.degree)
        .setMaxParallelism(slicer.degree)
        .name("Verdict filter")
        .uid("verdict-filter")

    val printedVerdicts = filteredVerdicts.flatMap(new PrintingFunction(new MonpolyVerdictFormatter))
        .setParallelism(slicer.degree)
        .setMaxParallelism(slicer.degree)
        .name("Verdict printer")
        .uid("verdict-printer")

    printedVerdicts
  }
}
