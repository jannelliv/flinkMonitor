package ch.eth.inf.infsec

import ch.eth.inf.infsec.monitor.{EchoProcess, ExternalProcessOperator, MonpolyProcess}
import ch.eth.inf.infsec.policy.{Formula, Policy}
import ch.eth.inf.infsec.slicer.ColissionlessKeyGenerator
import ch.eth.inf.infsec.trace._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

import scala.io.Source


object StreamMonitoring {

  private val logger = LoggerFactory.getLogger(StreamMonitoring.getClass)

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

    env.getConfig.setLatencyTrackingInterval(1000)

    // Performance tuning
    env.getConfig.enableObjectReuse()
    env.registerType(classOf[StringValue])
    env.registerType(classOf[IntegralValue])

    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val sliceMapping = ColissionlessKeyGenerator.getMapping(processors)

    //Single node
    val textStream = in match {
      case Some(Left((h, p))) => env.socketTextStream(h, p).uid("socket-source")
      case Some(Right(f)) =>
        if (watchInput)
          env.readFile(new TextInputFormat(new Path(f)), f, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
            .uid("watching-file-source")
        else
          env.readTextFile(f).uid("simple-file-source")
      case _ => logger.error("Cannot parse the input argument"); sys.exit(1)
    }
    val parsedTrace = textStream.flatMap(new ProcessorFunction(inputFormat.createParser())).uid("input-parser")

    val slicedTrace = parsedTrace
      .flatMap(new ProcessorFunction(slicer)).uid("slicer")
      .map(x => (sliceMapping(x._1), x._2)).uid("key-remapper")
      .keyBy(x => x._1)

    // Parallel node
    // TODO(JS): Timeout? Capacity?
    val verdicts = ExternalProcessOperator.transform[(Int, Record), Int, String, String, String](
      sliceMapping,
      slicedTrace,
      new KeyedMonpolyPrinter[Int],
      process,
      if (isMonpoly) new MonpolyVerdictFilter(slicer.mkVerdictFilter) else StatelessProcessor.identity,
      256).setParallelism(processors).uid("monitor")

    //Single node

    out match {
      case Some(Left((h, p))) => verdicts
        .map(v => v + "\n").setParallelism(1).uid("add-newline")
        .writeToSocket(h, p, new SimpleStringSchema()).setParallelism(1).uid("socket-sink")
      case Some(Right(f)) => verdicts.writeAsText(f).setParallelism(1).uid("file-sink")
      case _ => verdicts.print().setParallelism(1).uid("print-sink")
    }
    env.execute("Parallel Online Monitor")
  }

}


//val verdicts = slicedTrace.reduce(monpoly)

//Type issue example with sized
//    val r:Relation = Set(Sized(SInteger(1),SString("a")),
//                         Sized(SInteger(2),SString("b")))
//    val e:Event = (4,Set(r))



