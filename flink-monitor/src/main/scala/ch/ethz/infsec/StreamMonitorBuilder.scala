package ch.ethz.infsec

import ch.ethz.infsec.autobalancer.{AllState, ConstantHistogram, DeciderFlatMapSimple, KnowledgeExtract, OutsideInfluence, StatsHistogram}
import ch.ethz.infsec.kafka.MonitorKafkaConfig
import ch.ethz.infsec.monitor.{ExternalProcess, ExternalProcessOperator, Fact}
import ch.ethz.infsec.slicer.{HypercubeSlicer, VerdictFilter}
import ch.ethz.infsec.tools.{AddSubtaskIndexFunction, DebugMap, ParallelSocketTextStreamFunction, ReorderFunction, TestSimpleStringSchema}
import ch.ethz.infsec.trace.formatter.MonpolyVerdictFormatter
import ch.ethz.infsec.trace.parser.TraceParser
import ch.ethz.infsec.trace.{ParsingFunction, PrintingFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.IdPartitioner
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SocketClientSink}
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * Helper class that assembles the parallel monitor's data flow.
 */
class StreamMonitorBuilder(env: StreamExecutionEnvironment, reorder: ReorderFunction) {
  def socketSource(host: String, port: Int): DataStream[String] = {
      env
        .addSource(new ParallelSocketTextStreamFunction(host, port))
        .setParallelism(StreamMonitoring.inputParallelism)
        .setMaxParallelism(StreamMonitoring.inputParallelism)
        .name("Socket source")
        .uid("socket-source")
  }

  def fileWatchSource(path: String): DataStream[String] = {
    env.readFile(new TextInputFormat(new Path(path)), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
      .name("File watch source")
      .uid("file-watch-source")
  }

  def kafkaSource() : DataStream[String] = {
    val source = new FlinkKafkaConsumer011[String](MonitorKafkaConfig.getTopic,
      new TestSimpleStringSchema,
      MonitorKafkaConfig.getKafkaProps)
    source.setStartFromEarliest()

    env
      .addSource(source)
      .setParallelism(StreamMonitoring.inputParallelism)
      .setMaxParallelism(StreamMonitoring.inputParallelism)
      .name("Kafka source")
      .uid("kafka-source")
  }

  def simpleFileSource(path: String): DataStream[String] = {
    env.readTextFile(path)
      .name("Simple file source")
      .uid("simple-file-source")
  }

  def socketSink(verdicts: DataStream[String], host: String, port: Int): Unit = {
    val sink = new SocketClientSink[String](host, port, new SimpleStringSchema(), 0)
    verdicts
      .addSink(sink)
      .setParallelism(1)
      .name("Socket sink")
      .uid("socket-sink")
  }

  def fileSink(verdicts: DataStream[String], path: String): Unit = {
    verdicts
      .addSink(new BucketingSink[String](path))
      .setParallelism(1)
      .name("File sink")
      .uid("file-sink")
  }

  def printSink(verdicts: DataStream[String]): Unit = {
    verdicts
      .addSink(new PrintSinkFunction[String]())
      .setParallelism(1)
      .name("Print sink")
      .uid("print-sink")
  }

  def assembleDeciderIteration(parsedTrace: DataStream[Fact],
                               slicer: HypercubeSlicer,
                               monitorProcess: ExternalProcess,
                               shouldReorder: Boolean,
                               queueSize: Int,
                               windowSize: Double): DataStream[Fact] = {
    val decider = new AllState(new DeciderFlatMapSimple(slicer.degree, StreamMonitoring.inputParallelism, slicer.formula))
    val iterateFun = (iteration: DataStream[Fact]) => {
      val sampledTrace = iteration
        .process(new OutsideInfluence(slicer.degree, slicer.formula, windowSize))
        .name("outside influence")
        .uid("outside-influence")
        .setMaxParallelism(StreamMonitoring.inputParallelism)
        .setParallelism(StreamMonitoring.inputParallelism)

      val samplingOutput = sampledTrace
        .getSideOutput(OutsideInfluence.statsOutput)

      val parsedAndSlicedTrace = sampledTrace.flatMap(slicer)
        .setMaxParallelism(StreamMonitoring.inputParallelism)
        .setParallelism(StreamMonitoring.inputParallelism)
        .name("Slicer")
        .uid("slicer")

      val indexTrace = parsedAndSlicedTrace
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

      val reorderedTrace =
        if (shouldReorder) {
          partitionedTraceWithoutId
            .flatMap(reorder)
            .setParallelism(slicer.degree)
            .setMaxParallelism(slicer.degree)
            .name("Reorder facts")
            .uid("reorder-facts")
        } else {
          partitionedTraceWithoutId
            .map(_._2)
        }

      val rawVerdicts = ExternalProcessOperator.transform(reorderedTrace, monitorProcess, queueSize)
        .setParallelism(slicer.degree)
        .setMaxParallelism(slicer.degree)
        .name("Monitor")
        .uid("monitor")

      val rawVerdictsWithSideEffects = rawVerdicts
        .process(new KnowledgeExtract(slicer.degree))
        .setParallelism(slicer.degree)
        .setMaxParallelism(slicer.degree)
        .name("Knowledge Extractor")
        .uid("knowledge-extractor")

      val knowledgeExtractorCommandOutput = rawVerdictsWithSideEffects
        .getSideOutput(KnowledgeExtract.commandOutput)

      val feedBack = knowledgeExtractorCommandOutput
        .union(samplingOutput)
        .flatMap(decider)
        .setParallelism(1)
        .setMaxParallelism(1)
        .name("Decider")
        .uid("decider")
        .broadcast
        .map(k => k)
        .setParallelism(StreamMonitoring.inputParallelism)
        .setMaxParallelism(StreamMonitoring.inputParallelism)

      (feedBack, rawVerdictsWithSideEffects)
    }

    parsedTrace.iterate(iterateFun, 20000L)
  }

  def assembleWithoutDecider(parsedTrace: DataStream[Fact],
                             slicer: HypercubeSlicer,
                             monitorProcess: ExternalProcess,
                             shouldReorder: Boolean,
                             queueSize: Int): DataStream[Fact] = {

      val parsedAndSlicedTrace =
        parsedTrace
          .flatMap(slicer)
          .setMaxParallelism(StreamMonitoring.inputParallelism)
          .setParallelism(StreamMonitoring.inputParallelism)
          .name("Slicer")
          .uid("slicer")

      val indexTrace =
        parsedAndSlicedTrace
          .flatMap(new AddSubtaskIndexFunction)
          .setMaxParallelism(StreamMonitoring.inputParallelism)
          .setParallelism(StreamMonitoring.inputParallelism)
          .name("Add subtask index")
          .uid("subtask_index")

      val partitionedTraceWithoutId =
        indexTrace
          .partitionCustom(new IdPartitioner, 0)
          .map(k => (k._2, k._3))
          .setParallelism(slicer.degree)
          .setMaxParallelism(slicer.degree)
          .name("Partition and remove slice ID")
          .uid("remove-id")

      val reorderedTrace =
        if (shouldReorder) {
          partitionedTraceWithoutId
            .flatMap(reorder)
            .setParallelism(slicer.degree)
            .setMaxParallelism(slicer.degree)
            .name("Reorder facts")
            .uid("reorder-facts")
        } else {
          partitionedTraceWithoutId
            .map(_._2)
        }
          .map(new DebugMap[Fact])
          .setParallelism(slicer.degree)
          .setMaxParallelism(slicer.degree)
          .uid("debug-map")

      ExternalProcessOperator.transform(reorderedTrace, monitorProcess, queueSize)
        .setParallelism(slicer.degree)
        .setMaxParallelism(slicer.degree)
        .name("Monitor")
        .uid("monitor")
  }

  def assemble(inputStream: DataStream[String],
               traceFormat: TraceParser,
               decider: Boolean,
               shouldReorder: Boolean,
               slicer: HypercubeSlicer,
               monitorProcess: ExternalProcess,
               queueSize: Int,
               windowSize: Double): DataStream[String] = {
    val parsedTrace =
      inputStream
        .flatMap(new ParsingFunction(traceFormat))
        .name("Trace parser")
        .uid("trace-parser")
        .setMaxParallelism(StreamMonitoring.inputParallelism)
        .setParallelism(StreamMonitoring.inputParallelism)
        /*.map(new DebugMap[Fact])
        .setParallelism(StreamMonitoring.inputParallelism)
        .setMaxParallelism(StreamMonitoring.inputParallelism)
        .uid("debug-map")*/

    val rawVerdicts = {
      if (decider) assembleDeciderIteration(parsedTrace, slicer, monitorProcess, shouldReorder, queueSize, windowSize)
      else assembleWithoutDecider(parsedTrace, slicer, monitorProcess, shouldReorder, queueSize)
    }

    val filteredVerdicts =
      rawVerdicts
        .filter(new VerdictFilter(slicer))
        .setParallelism(slicer.degree)
        .setMaxParallelism(slicer.degree)
        .name("Verdict filter")
        .uid("verdict-filter")

    val latencyTracking =
      filteredVerdicts
        .flatMap(new LatencyProcessingFunction)
        .setParallelism(1)
        .setMaxParallelism(1)
        .name("Latency tracking")
        .uid("latency-tracking")

    latencyTracking.flatMap(new PrintingFunction(new MonpolyVerdictFormatter))
      .setParallelism(1)
      .setMaxParallelism(1)
      .name("Verdict printer")
      .uid("verdict-printer")
  }
}
