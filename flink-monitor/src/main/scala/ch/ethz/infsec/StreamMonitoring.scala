package ch.ethz.infsec

import java.io._
import java.lang.ProcessBuilder.Redirect
import java.util.concurrent.{Callable, Executors, TimeUnit}

import ch.ethz.infsec.analysis.TraceAnalysis
import ch.ethz.infsec.kafka.MonitorKafkaConfig
import ch.ethz.infsec.monitor._
import ch.ethz.infsec.policy.{Formula, Policy}
import ch.ethz.infsec.tools.{EndPoint, FileEndPoint, KafkaEndpoint, KafkaTestProducer, MultiSourceVariant, PerPartitionOrder, SocketEndpoint, TotalOrder, WaterMarkOrder}
import ch.ethz.infsec.trace.parser.{Crv2014CsvParser, MonpolyTraceParser, TraceParser}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory
import org.slf4j.helpers.MessageFormatter

import scala.collection.JavaConverters
import scala.io.Source

object StreamMonitoring {

  private val logger = LoggerFactory.getLogger(StreamMonitoring.getClass)

  private val MONPOLY_CMD = "monpoly"
  private val ECHO_DEJAVU_CMD = "echo-dejavu"
  private val ECHO_MONPOLY_CMD = "echo-monpoly"
  private val DEJAVU_CMD = "dejavu"

  var jobName: String = ""

  var checkpointUri: String = ""
  var in: Option[EndPoint] = _
  var out: Option[EndPoint] = _
  var inputFormat: TraceParser = _
  var watchInput: Boolean = false
  var kafkaTestFile: String = ""

  var processors: Int = 0

  var monitorCommand: String = ""
  var command: Seq[String] = Seq.empty
  var formulaFile: String = ""
  var signatureFile: String = ""
  var initialStateFile: Option[String] = None
  var negate: Boolean = false
  var simulateKafkaProducer: Boolean = false
  var queueSize = 10000
  var inputParallelism = 1
  var clearTopic = false
  var multiSourceVariant: MultiSourceVariant = TotalOrder()

  var formula: Formula = policy.False()

  private def fail(message: String, values: Object*): Nothing = {
    logger.error(message, values: _*)
    System.err.println("Error: " + MessageFormatter.arrayFormat(message, values.toArray).getMessage)
    sys.exit(1)
  }

  def parseEndpointArg(ss: String): Option[EndPoint] = {
    /**
     * IP:Port        OR
     * kafka          OR
     * filename
     */
    if (ss.equalsIgnoreCase("kafka")) {
      return Some(KafkaEndpoint())
    }
    try {
      if (ss.isEmpty) None
      else {
        val s = ss.split(":")
        if (s.length > 1) {
          val p = s(1).toInt
          Some(SocketEndpoint(s(0), p))
        } else {
          Some(FileEndPoint(ss))
        }
      }
    }
    catch {
      case _: Exception => None
    }
  }

  def init(params: ParameterTool) {
    clearTopic = params.getBoolean("clear", false)
    inputParallelism = params.getInt("nparts", -1)
    jobName = params.get("job", "Parallel Online Monitor")
    kafkaTestFile = params.get("kafkatestfile", "")

    checkpointUri = params.get("checkpoints", "")

    in = parseEndpointArg(params.get("in", "127.0.0.1:9000"))
    out = parseEndpointArg(params.get("out", ""))

    inputFormat = params.get("format", "monpoly") match {
      case "monpoly" => new MonpolyTraceParser
      case "csv" => new Crv2014CsvParser
      //case "dejavu" => DejavuFormat
      case format => fail("Unknown trace format " + format)
    }

    watchInput = params.getBoolean("watch", false)

    processors = params.getInt("processors", 1)
    logger.info(s"Using $processors parallel monitors")

    negate = params.getBoolean("negate", false)
    monitorCommand = params.get("monitor", MONPOLY_CMD)
    multiSourceVariant = params.getInt("multi", 3) match {
      case 1 => TotalOrder()
      case 2 => PerPartitionOrder()
      case 3 => WaterMarkOrder()
      case 4 => WaterMarkOrder()
      case _ => fail("multisourcevariant parameter must be between 1 and 4")
    }

    val commandString = params.get("command")
    command = if (commandString == null) Seq.empty else commandString.split(' ')
    signatureFile = params.get("sig")
    formulaFile = params.get("formula")
    val formulaSource = Source.fromFile(formulaFile).mkString
    formula = Policy.read(formulaSource) match {
      case Left(err) => fail("Cannot parse the formula: " + err)
      case Right(phi) => phi
    }

    initialStateFile = Option(params.get("load"))

    queueSize = params.getInt("queueSize", queueSize)
  }

  /*
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
    var content = Source.fromFile(file).mkString.split("\n").map(_.trim)
    val parser = MonpolyFormat.createParser()
   // var arr = scala.collection.mutable.ArrayBuffer[Record]()
    var arr = parser.processAll(content)
    //explicit nulling to aid the JVM in GCing
    content = null

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
    var outs = strategy.processAll(arr)
    //explicit nulling to aid the JVM in GCing
    arr = null
    var sortedOuts = scala.collection.mutable.Map[Int,scala.collection.mutable.ArrayBuffer[Record]]()
    outs.foreach(x => {
        if(!sortedOuts.contains(x._1))
          sortedOuts += ((x._1,scala.collection.mutable.ArrayBuffer[Record]()))
        sortedOuts(x._1) += x._2
    })
    //explicit nulling to aid the JVM in GCing
    outs = null
    var printer = new KeyedMonpolyPrinter[Int, Nothing](false)
    sortedOuts.foreach(x => {
      val w2 = new java.io.PrintWriter("slicedOutput"+x._1)
      x._2.foreach(rec => {
        printer.process(Left((x._1,rec)), {case Left(y) => w2.println(y.in); case Right(_) => ()})
      })
      w2.close()
    })
    sortedOuts = null
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

  def exename(str: String):String = {
    val ss = str.split('/')
    ss(ss.length-1)
  }
   */


  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)

    val analysis = params.getBoolean("analysis", false)
    //val calcSlice = params.getBoolean("calcSlice",false)

    if (analysis) TraceAnalysis.prepareSimulation(params)
    //else if(calcSlice) calculateSlicing(params)
    else {
      init(params)

      if (formula.freeVariables.isEmpty) {
        fail("Closed formula cannot be sliced")
      }
      //TODO: replace
      val slicer = SlicingSpecification.mkSlicer(params, formula, processors)
      //val slicer = SlicingSpecification.mkSlicer(params, formula, 8)

      val monitorProcess: ExternalProcess[Fact, Fact] = monitorCommand match {
        case MONPOLY_CMD => {
          val monitorArgs = if (command.nonEmpty) command else (if (negate) monitorCommand :: List("-negate") else List(monitorCommand))
          val margs = monitorArgs ++ List("-sig", signatureFile, "-formula", formulaFile)
          logger.info("Monitor command: {}", margs.mkString(" "))
          new MonpolyProcess(margs, initialStateFile)
        }
        case ECHO_DEJAVU_CMD => {
          val monitorArgs = if (command.nonEmpty) command else List(monitorCommand)
          logger.info("Monitor command: {}", monitorArgs.mkString(" "))
          new EchoDejavuProcess(monitorArgs)
        }
        case ECHO_MONPOLY_CMD => {
          val monitorArgs = if (command.nonEmpty) command else List(monitorCommand)
          logger.info("Monitor command: {}", monitorArgs.mkString(" "))
          new EchoMonpolyProcess(monitorArgs)
        }
        case DEJAVU_CMD => {
          if (slicer.requiresFilter) {
            fail("Formulas containing equality or duplicate predicate names cannot be used with DejaVu")
          }

          val monitorArgs = if (command.nonEmpty) command else List(monitorCommand)
          val dejavuFormulaFile = formulaFile + ".qtl"
          val writer = new PrintWriter(new FileOutputStream(dejavuFormulaFile, false))
          writer.write(formula.toQTLString(!negate))
          writer.close()

          //Compiling the monitor
          val compileArgs = monitorArgs ++ List("compile", dejavuFormulaFile)
          val process = new ProcessBuilder(JavaConverters.seqAsJavaList(compileArgs))
            .redirectError(Redirect.INHERIT)
            .start()
          val reader = new BufferedReader(new InputStreamReader(process.getInputStream))

          val readerThread = new Callable[String] {
            override def call(): String = reader.readLine()
          }

          try {
            val executor = Executors.newFixedThreadPool(1)
            val monitorLocation = executor.submit(readerThread).get(20, TimeUnit.SECONDS)
            executor.shutdown()
            val runArgs = monitorArgs ++ List("run", monitorLocation, "25")
            logger.info("Monitor command: {}", runArgs.mkString(" "))
            new DejavuProcess(runArgs)
          } catch {
            case e: Exception => {
              fail("Dejavu monitor compilation failed: {}", e.getMessage)
            }
          }
        }
        case c@_ => fail("Cannot parse the monitor command: {}", c)
      }

      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

      if (!checkpointUri.isEmpty) {
        env.setStateBackend(new RocksDBStateBackend(checkpointUri))
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE)
      }
      env.setRestartStrategy(RestartStrategies.noRestart())

      // Disable automatic latency tracking, since we inject latency markers ourselves.
      env.getConfig.setLatencyTrackingInterval(-1)

      // Performance tuning
      env.getConfig.enableObjectReuse()
      env.getConfig.registerTypeWithKryoSerializer(classOf[Fact], new FactSerializer)

      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      var monitor: StreamMonitorBuilder = new StreamMonitorBuilderSimple(env)
      // textStream = env.addSource[String](helpers.createStringConsumerForTopic("flink_input","localhost:9092","flink"))
      val textStream = in match {
        case Some(SocketEndpoint(h, p)) =>
          monitor = multiSourceVariant.getStreamMonitorBuilder(env)
          inputFormat.setTerminatorMode(multiSourceVariant.getTerminatorMode)
          Thread.sleep(4000)
          monitor.socketSource(h, p)
        case Some(FileEndPoint(f)) => if (watchInput) monitor.fileWatchSource(f) else monitor.simpleFileSource(f)
        case Some(KafkaEndpoint()) =>
          MonitorKafkaConfig.init(clearTopic = clearTopic, numPartitions = Some(inputParallelism))
          inputParallelism = MonitorKafkaConfig.getNumPartitions
          if (!kafkaTestFile.isEmpty) {
            val PrefixRegex = """(.*)/(.*)""".r
            val PrefixRegex(kafkaDir, kafkaPrefix) = kafkaTestFile
            val testProducer = new KafkaTestProducer(kafkaDir, kafkaPrefix)
            testProducer.runProducer(true)
          }
          monitor = multiSourceVariant.getStreamMonitorBuilder(env)
          inputFormat.setTerminatorMode(multiSourceVariant.getTerminatorMode)
          monitor.kafkaSource()
        case _ => fail("Cannot parse the input argument")
      }
      env.setMaxParallelism(inputParallelism)
      env.setParallelism(inputParallelism)

      /*
      val dfms = if(params.has("decider"))
        Some(new DeciderFlatMapSimple(slicer.degree, formula, params.getInt("windowsize",100)))
      else
        None
       */
      val verdicts = monitor.assemble(textStream, inputFormat, slicer, monitorProcess, queueSize)

      out match {
        case Some(SocketEndpoint(h, p)) => monitor.socketSink(verdicts, h, p)
        case Some(FileEndPoint(f)) => monitor.fileSink(verdicts, f)
        case _ => monitor.printSink(verdicts)
      }

      /*
      Rescaler.create(jobName, "127.0.0.1", processors)
      Thread.sleep(2000)
       */
      env.execute(jobName)
    }
  }
}
