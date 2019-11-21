package ch.ethz.infsec

import ch.ethz.infsec.kafka.MonitorKafkaConfig
import ch.ethz.infsec.monitor.{ExternalProcess, ExternalProcessOperator, Fact}
import ch.ethz.infsec.slicer.{HypercubeSlicer, VerdictFilter}
import ch.ethz.infsec.tools.{AddSubtaskIndexFunction, DebugMap, ReorderFunction, ReorderTotalOrderFunction, TestSimpleStringSchema}
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

class StreamMonitorBuilderParInput(env: StreamExecutionEnvironment, reorder: ReorderFunction) extends StreamMonitorBuilder(env) {
  override protected def partitionAndReorder(dataStream: DataStream[(Int, Fact)], slicer: HypercubeSlicer): DataStream[Fact] = {
    val indexTrace = dataStream
      .flatMap(new AddSubtaskIndexFunction)
      .setMaxParallelism(StreamMonitoring.inputParallelism)
      .setParallelism(StreamMonitoring.inputParallelism)
      .name("Add subtask index")
      .uid("subtask_index")

    val partitionedTraceWithoutId = indexTrace
      .partitionCustom(new IdPartitioner, 0)
      .map(k => (k._2, k._3))
      .setParallelism(slicer.degree)
      .setMaxParallelism(slicer.degree)
      .name("Partition and remove slice ID")
      .uid("remove-id")

    partitionedTraceWithoutId
      .flatMap(reorder)
      .setParallelism(slicer.degree)
      .setMaxParallelism(slicer.degree)
      .name("Reorder facts")
      .uid("reorder-facts")
  }
}

class StreamMonitorBuilderSimple(env: StreamExecutionEnvironment) extends StreamMonitorBuilder(env) {
  override protected def partitionAndReorder(dataStream: DataStream[(Int, Fact)], slicer: HypercubeSlicer): DataStream[Fact] = {
    val indexTrace = dataStream
      .flatMap(new AddSubtaskIndexFunction)
      .setMaxParallelism(StreamMonitoring.inputParallelism)
      .setParallelism(StreamMonitoring.inputParallelism)
      .name("Add subtask index")
      .uid("subtask_index")

    val partitionedTraceWithoutId = indexTrace
      .partitionCustom(new IdPartitioner, 0)
      .map(k => (k._2, k._3))
      .setParallelism(slicer.degree)
      .setMaxParallelism(slicer.degree)
      .name("Partition and remove slice ID")
      .uid("remove-id")

    partitionedTraceWithoutId
      .flatMap(new ReorderTotalOrderFunction(StreamMonitoring.inputParallelism))
      .setParallelism(slicer.degree)
      .setMaxParallelism(slicer.degree)
      .name("Reorder facts")
      .uid("reorder-facts")
  }
}

/**
 * Helper class that assembles the parallel monitor's data flow.
 */
abstract class StreamMonitorBuilder(env: StreamExecutionEnvironment) {
  def socketSource(host: String, port: Int): DataStream[String] = {
    val rawSource = new SocketTextStreamFunction(host, port, "\n", 0)
    LatencyTrackingExtensions.addSourceWithProvidedMarkers(env, rawSource, "Socket source")
      .uid("socket-source")
  }

  def fileWatchSource(path: String): DataStream[String] = {
    env.readFile(new TextInputFormat(new Path(path)), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
      .name("File watch source")
      .uid("file-watch-source")
  }

  def kafkaSource() : DataStream[String] = {
    val rawSource = new FlinkKafkaConsumer011[String](MonitorKafkaConfig.getTopic,
      new TestSimpleStringSchema,
      MonitorKafkaConfig.getKafkaProps)
    rawSource.setStartFromEarliest()
    LatencyTrackingExtensions.addSourceWithProvidedMarkers(env, rawSource, "Kafka source")
      .setParallelism(StreamMonitoring.inputParallelism)
      .setMaxParallelism(StreamMonitoring.inputParallelism)
      .uid("kafka-source")
  }

  def simpleFileSource(path: String): DataStream[String] = {
    env.readTextFile(path)
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

  def parseAndSliceTrace(traceFormat: TraceParser, slicer: HypercubeSlicer, dataStream: DataStream[String]) : DataStream[(Int, Fact)] = {
    dataStream
      .map(new DebugMap[String]())
      .flatMap(new ParsingFunction(traceFormat))
      .name("Trace parser")
      .uid("trace-parser")
      .setMaxParallelism(StreamMonitoring.inputParallelism)
      .setParallelism(StreamMonitoring.inputParallelism)
      .flatMap(slicer)
      .setMaxParallelism(StreamMonitoring.inputParallelism)
      .setParallelism(StreamMonitoring.inputParallelism)
      .name("Slicer")
      .uid("slicer")
  }

  def postProcessStream(slicer: HypercubeSlicer, queueSize: Int, monitorProcess: ExternalProcess[Fact, Fact], dataStream: DataStream[Fact]) : DataStream[String] = {
    val rawVerdicts = ExternalProcessOperator.transform(dataStream, monitorProcess, queueSize)
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

  protected def partitionAndReorder(dataStream: DataStream[(Int, Fact)], slicer: HypercubeSlicer) : DataStream[Fact]

  def assemble(inputStream: DataStream[String],
               traceFormat: TraceParser,
//               decider: Option[DeciderFlatMapSimple],
               slicer: HypercubeSlicer,
               monitorProcess: ExternalProcess[Fact, Fact],
               queueSize: Int): DataStream[String] = {

//    val parser = new TraceMonitor(traceFormat.createParser(), new RescaleInitiator().rescale)
    val parsedAndSlicedTrace = parseAndSliceTrace(traceFormat, slicer, inputStream)

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

    val reorderedTrace = partitionAndReorder(parsedAndSlicedTrace, slicer)


    // XXX(JS): Implement this comment here:
    // We do not send any commands to unused submonitors. In particular, we cannot use their state fragments
    // because the state is not synchronized with the global progress. Ideally, we would not even create
    // such submonitors.
    // TODO(JS): Check the splitting/merging logic in the case of unused submonitors.
    postProcessStream(slicer, queueSize, monitorProcess, reorderedTrace)

  }
}
