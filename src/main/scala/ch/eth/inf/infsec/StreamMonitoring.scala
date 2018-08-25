package ch.eth.inf.infsec

import ch.eth.inf.infsec.monitor.{EchoProcess, ExternalProcessOperator, MonpolyProcess, MonpolyRequest}
import ch.eth.inf.infsec.policy.{Formula, Policy}
import ch.eth.inf.infsec.slicer.ColissionlessKeyGenerator
import ch.eth.inf.infsec.trace._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.IdPartitioner
import org.apache.flink.api.java.io.{TextInputFormat, TextOutputFormat}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{OutputFormatSinkFunction, PrintSinkFunction, SocketClientSink}
import org.apache.flink.streaming.api.functions.source.{FileProcessingMode, SocketTextStreamFunction}
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

import scala.io.Source


object StreamMonitoring {

  private val logger = LoggerFactory.getLogger(StreamMonitoring.getClass)

  var jobName: String = ""

  var checkpointUri: String = ""

  var in: Option[Either[(String, Int), String]] = _
  var out: Option[Either[(String, Int), String]] = _
  var inputFormat: TraceFormat = _
  var watchInput: Boolean = false

  var processorExp: Int = 0
  var processors: Int = 0

  var monitorCommand: Seq[String] = Seq.empty
  var isMonpoly: Boolean = true
  var formulaFile: String = ""
  var signatureFile: String = ""

  var formula: Formula = policy.False()

  def floorLog2(x: Int): Int = {
    var remaining = x
    var y = -1
    while (remaining > 0) {
      remaining >>= 1
      y += 1
    }
    y
  }

  def parseArgs(ss: String): Option[Either[(String, Int), String]] = {
    try {
      if (ss.isEmpty) None
      else {
        val s = ss.split(":")
        if (s.length > 1) {
          val p = s(1).toInt
          Some(Left(s(0), p))
        } else {
          Some(Right(ss))
        }
      }
    }
    catch {
      case _: Exception => None
    }
  }

  def init(params: ParameterTool) {
    jobName = params.get("job", "Parallel Online Monitor")

    checkpointUri = params.get("checkpoints", "")

    in = parseArgs(params.get("in", "127.0.0.1:9000"))
    out = parseArgs(params.get("out", ""))

    inputFormat = params.get("format", "monpoly") match {
      case "monpoly" => MonpolyFormat
      case "csv" => CsvFormat
      case format =>
        logger.error("Unknown trace format " + format)
        sys.exit(1)
    }

    watchInput = params.getBoolean("watch", false)

    val requestedProcessors = params.getInt("processors", 1)
    processorExp = floorLog2(requestedProcessors).max(0)
    processors = 1 << processorExp
    if (processors != requestedProcessors) {
      logger.warn(s"Number of processors is not a power of two, using $processors instead")
    }
    logger.info(s"Using $processors parallel monitors")

    monitorCommand = params.get("monitor", "monpoly -negate").split(' ')
    isMonpoly = params.getBoolean("monpoly", true)
    signatureFile = params.get("sig")

    formulaFile = params.get("formula")
    val formulaSource = Source.fromFile(formulaFile).mkString
    formula = Policy.read(formulaSource) match {
      case Left(err) =>
        logger.error("Cannot parse the formula: " + err)
        sys.exit(1)
      case Right(phi) => phi
    }
  }

  def main(args: Array[String]) {

    val params = ParameterTool.fromArgs(args)
    init(params)

    val slicer = SlicingSpecification.mkSlicer(params, formula, processors)

    val monitorArgs = monitorCommand ++ List("-sig", signatureFile, "-formula", formulaFile)
    logger.info("Monitor command: {}", monitorArgs.mkString(" "))
    val process = if (isMonpoly) new MonpolyProcess(monitorArgs) else new EchoProcess(monitorArgs)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    if (!checkpointUri.isEmpty) {
      env.setStateBackend(new RocksDBStateBackend(checkpointUri))
      env.enableCheckpointing(10000)
    }
    env.setRestartStrategy(RestartStrategies.noRestart())

    // Disable automatic latency tracking, since we inject latency markers ourselves.
    env.getConfig.setLatencyTrackingInterval(-1)

    // Performance tuning
    env.getConfig.enableObjectReuse()

    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    //Single node
    val textStream = in match {
      case Some(Left((h, p))) => LatencyTrackingExtensions.addSourceWithProvidedMarkers(
          env,
          new SocketTextStreamFunction(h, p, "\n", 0),
          "Socket source")
        .uid("socket-source")
      case Some(Right(f)) =>
        if (watchInput)
          env.readFile(new TextInputFormat(new Path(f)), f, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
            .name("File watch source")
            .uid("file-watch-source")
        else
          env.readTextFile(f).name("file-source").uid("File source")
      case _ => logger.error("Cannot parse the input argument"); sys.exit(1)
    }
    val parsedTrace = textStream.flatMap(new ProcessorFunction(inputFormat.createParser()))
      .name("Parser")
      .uid("input-parser")

    val slicedTrace = parsedTrace
      .flatMap(new ProcessorFunction(slicer)).name("Slicer").uid("slicer")
      .partitionCustom(new IdPartitioner, 0)

    // Parallel node
    // TODO(JS): Timeout? Capacity?
    val verdicts = ExternalProcessOperator.transform[(Int, Record), MonpolyRequest, String, String](
      slicer,
      slicedTrace,
      new KeyedMonpolyPrinter[Int],
      process,
      if (isMonpoly) new MonpolyVerdictFilter(slicer.mkVerdictFilter) else StatelessProcessor.identity,
      256).setParallelism(processors).name("Monitor").uid("monitor")

    //Single node

    out match {
      case Some(Left((h, p))) =>
        val outVerdicts = verdicts.map(v => v + "\n").setParallelism(1).name("Add newline").uid("add-newline")
        LatencyTrackingExtensions.addPreciseLatencyTrackingSink(
            outVerdicts,
            new SocketClientSink[String](h, p, new SimpleStringSchema(), 0))
          .setParallelism(1).name("Socket sink").uid("socket-sink")
      case Some(Right(f)) =>
        LatencyTrackingExtensions.addPreciseLatencyTrackingSink(
            verdicts,
            new OutputFormatSinkFunction[String](new TextOutputFormat[String](new Path(f))))
          .setParallelism(1).name("File sink").uid("file-sink")
      case _ =>
        LatencyTrackingExtensions.addPreciseLatencyTrackingSink(
            verdicts,
            new PrintSinkFunction[String]())
          .setParallelism(1).name("Print sink").uid("print-sink")
    }

    env.execute(jobName)
  }

}
