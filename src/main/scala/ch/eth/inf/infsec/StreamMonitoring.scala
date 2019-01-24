package ch.eth.inf.infsec

import java.io.PrintWriter

import ch.eth.inf.infsec.autobalancer.DeciderFlatMapSimple
import ch.eth.inf.infsec.slicer.{HypercubeSlicer, SlicerParser}
import ch.eth.inf.infsec.analysis.TraceAnalysis
import ch.eth.inf.infsec.monitor._
import ch.eth.inf.infsec.policy.{Formula, Policy}
import ch.eth.inf.infsec.tools.Rescaler
import ch.eth.inf.infsec.tools.Rescaler.RescaleInitiator
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

  def calculateSlicing(params: ParameterTool): Unit =
  {
    val degree = params.getInt("degree",4)
    formulaFile = params.get("formula")
    val formulaSource = Source.fromFile(formulaFile).mkString
    formula = Policy.read(formulaSource) match {
      case Left(err) =>
        logger.error("Cannot parse the formula: " + err)
        sys.exit(1)
      case Right(phi) => phi
    }
    var dfms = new DeciderFlatMapSimple(degree,formula,Double.MaxValue)
    val file = params.get("file")
    val content = Source.fromFile(file).mkString.split("\n").map(_.trim)
    val parser = MonpolyFormat.createParser()
   // var arr = scala.collection.mutable.ArrayBuffer[Record]()
    val arr = parser.processAll(content)

    val slicer = params.get("slicer",null)
    var strategy : HypercubeSlicer = null
    if(slicer == null) {
      arr.foreach(x => {
        if (!x.isEndMarker)
          dfms.windowStatistics.addEvent(x)
      })
      dfms.windowStatistics.nextFrame()
      strategy = dfms.getSlicingStrategy(dfms.windowStatistics)
    }else{
      val res = new SlicerParser().parseSlicer(slicer)
      strategy = new HypercubeSlicer(formula,res._1,res._2,1234)
      strategy.parseSlicer(slicer)
    }
    val strategyStr = strategy.stringify()
    val writer = new java.io.PrintWriter("strategyOutput")
    writer.println(strategyStr)
    writer.close()
    val outs = strategy.processAll(arr)
    val sortedOuts = scala.collection.mutable.Map[Int,scala.collection.mutable.ArrayBuffer[Record]]()
    outs.foreach(x => {
        if(!sortedOuts.contains(x._1))
          sortedOuts += ((x._1,scala.collection.mutable.ArrayBuffer[Record]()))
        sortedOuts(x._1) += x._2
    })
    var printer = new KeyedMonpolyPrinter[Int]
    sortedOuts.foreach(x => {
      val w2 = new java.io.PrintWriter("slicedOutput"+x._1)
      x._2.foreach(rec => {
        printer.process((x._1,rec),y => w2.println(y.in))
      })
      w2.close()
    })
    val w3 = new PrintWriter("otherStuff")
    w3.println("relationSet size: "+dfms.windowStatistics.relations.size)
    w3.println("heavy hitter size: "+dfms.windowStatistics.heavyHitter.values.size)
    w3.println("heavy hitters:")
    dfms.windowStatistics.heavyHitter.foreach(x => {
      w3.println(x)
    })
    w3.println("relation sizes:")
    dfms.windowStatistics.relations.foreach(x => {
      w3.println(x._1 + " - " + x._2 + "(relation) " + dfms.windowStatistics.relationSize(x._1) + "(relationSize)")
    })
    w3.close()
  }

  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)

    val analysis = params.getBoolean("analysis", false)
    val calcSlice = params.getBoolean("calcSlice",false)

    if (analysis) TraceAnalysis.prepareSimulation(params)
    else if(calcSlice) calculateSlicing(params)
    else{
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
      env.setMaxParallelism(1)
      env.setParallelism(1)

      //Single node
      val textStream = in match {
        case Some(Left((h, p))) =>
          LatencyTrackingExtensions.addSourceWithProvidedMarkers(
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
      val parsedTrace = textStream.flatMap(new ProcessorFunction(new TraceMonitor(inputFormat.createParser(), new RescaleInitiator().rescale)))
        .name("Parser")
        .uid("input-parser")
        .setMaxParallelism(1)
        .setParallelism(1)

      val injectedTrace = parsedTrace.flatMap(new ProcessorFunction(new OutsideInfluence()))

      //todo: proper arguments
      //assumes in-order atm
      val observedTrace = injectedTrace.flatMap (new DeciderFlatMapSimple(slicer.degree,formula,5)).setMaxParallelism(1)
        .setMaxParallelism(1)
        .setParallelism(1)//.name("ObservedTrace").uid("observed-trace");

      val slicedTrace = observedTrace
        .flatMap(new ProcessorFunction(slicer)).name("Slicer").uid("slicer")
        .setMaxParallelism(1)
        .setParallelism(1)
        .partitionCustom(new IdPartitioner, 0)

      // Parallel node
      // TODO(JS): Timeout? Capacity?
      val verdictsAndOtherThings = ExternalProcessOperator.transform[(Int, Record), MonpolyRequest, MonpolyRequest, MonpolyRequest](
        slicer,
        slicedTrace,
        new KeyedMonpolyPrinter[Int],
        process,
        if (isMonpoly) new LiftProcessor(new MonpolyVerdictFilter(slicer.mkVerdictFilter)) else StatelessProcessor.identity,
        256).setParallelism(processors).setMaxParallelism(processors).name("Monitor").uid("monitor")

      //Single node

      val verdicts = verdictsAndOtherThings.flatMap(new KnowledgeExtract())

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

      Rescaler.create(jobName, "127.0.0.1", processors)
      Thread.sleep(2000)
      env.execute(jobName)
    }
  }

}
