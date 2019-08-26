package ch.ethz.infsec

import java.io._
import java.lang.ProcessBuilder.Redirect
import java.util.concurrent.{Callable, Executors, TimeUnit}

import ch.ethz.infsec.analysis.TraceAnalysis
import ch.ethz.infsec.autobalancer.DeciderFlatMapSimple
import ch.ethz.infsec.monitor._
import ch.ethz.infsec.policy.{Formula, Policy}
import ch.ethz.infsec.slicer.{HypercubeSlicer, SlicerParser}
import ch.ethz.infsec.tools.Rescaler
import ch.ethz.infsec.trace.{CsvFormat, MonpolyFormat, _}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory
import org.slf4j.helpers.MessageFormatter

import scala.collection.JavaConverters
import scala.io.Source


object StreamMonitoring {

  private val logger = LoggerFactory.getLogger(StreamMonitoring.getClass)

  private val MONPOLY_CMD = "monpoly"
  private val ECHO_DEJAVU_CMD    = "echo-dejavu"
  private val ECHO_MONPOLY_CMD = "echo-monpoly"
  private val DEJAVU_CMD  = "dejavu"
  private val CUSTOM_CMD  = "custom"

  var jobName: String = ""

  var checkpointUri: String = ""

  var in: Option[Either[(String, Int), String]] = _
  var out: Option[Either[(String, Int), String]] = _
  var inputFormat: TraceFormat = _
  var markDatabaseEnd: Boolean = true
  var watchInput: Boolean = false

  var processors: Int = 0

  var monitorCommand: String = ""
  var command: Seq[String] = Seq.empty
  var formulaFile: String = ""
  var signatureFile: String = ""
  var initialStateFile: Option[String] = None
  var negate:Boolean = false
  var queueSize = 256


  var formula: Formula = policy.False()

  // FIXME(JS): Unused.
  /*
  def floorLog2(x: Int): Int = {
    var remaining = x
    var y = -1
    while (remaining > 0) {
      remaining >>= 1
      y += 1
    }
    y
  }
  */

  private def fail(message: String, values: Object*): Nothing = {
    logger.error(message, values:_*)
    System.err.println("Error: " + MessageFormatter.arrayFormat(message, values.toArray).getMessage)
    sys.exit(1)
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
      case "dejavu" => DejavuFormat
      case format => fail("Unknown trace format " + format)
    }
    markDatabaseEnd = params.getBoolean("end-marker", true)

    watchInput = params.getBoolean("watch", false)

    processors = params.getInt("processors", 1)
    logger.info(s"Using $processors parallel monitors")

    negate = params.getBoolean("negate",false)
    monitorCommand = params.get("monitor", MONPOLY_CMD)


    val commandString = params.get("command")
    command = if (commandString==null) Seq.empty else commandString.split(' ')
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


  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)

    val analysis = params.getBoolean("analysis", false)
    val calcSlice = params.getBoolean("calcSlice",false)

    if (analysis) TraceAnalysis.prepareSimulation(params)
    else if(calcSlice) calculateSlicing(params)
    else{
      init(params)

      if (formula.freeVariables.isEmpty) { fail("Closed formula cannot be sliced") }
      val slicer = SlicingSpecification.mkSlicer(params, formula, processors)


      val processFactory: MonitorFactory = monitorCommand match {
        case MONPOLY_CMD => {
          val monitorArgs = if (command.nonEmpty) command else (if (negate) monitorCommand::List("-negate") else List(monitorCommand))
          val margs = monitorArgs ++ List("-sig", signatureFile, "-formula", formulaFile)
          logger.info("Monitor command: {}", margs.mkString(" "))
          new MonpolyProcessFactory(margs, slicer, initialStateFile, markDatabaseEnd)
        }
        case ECHO_DEJAVU_CMD => {
          val monitorArgs = if (command.nonEmpty) command else List(monitorCommand)
          logger.info("Monitor command: {}", monitorArgs.mkString(" "))
          EchoDejavuProcessFactory(monitorArgs)
        }
        case ECHO_MONPOLY_CMD => {
          val monitorArgs = if (command.nonEmpty) command else List(monitorCommand)
          logger.info("Monitor command: {}", monitorArgs.mkString(" "))
          new EchoMonpolyProcessFactory(monitorArgs, markDatabaseEnd)
        }
        case DEJAVU_CMD => {
          if (slicer.requiresFilter) {
            fail("Formulas containing equality or duplicate predicate names cannot be used with DejaVu")
          }

          val monitorArgs = if (command.nonEmpty) command else List(monitorCommand)
          val dejavuFormulaFile = formulaFile+".qtl"
          val writer = new PrintWriter(new FileOutputStream(dejavuFormulaFile,false))
          writer.write(formula.toQTLString(!negate))
          writer.close()

          //Compiling the monitor
          val compileArgs = monitorArgs ++ List("compile",dejavuFormulaFile)
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
            DejavuProcessFactory(runArgs)
          } catch {
            case e : Exception => {
              fail("Dejavu monitor compilation failed: {}", e.getMessage)
            }
          }
        }
        case CUSTOM_CMD => {
          if (command.isEmpty) {
            fail("Custom monitor must have a --command argument")
          }
          logger.info("Monitor command: {}", command.mkString(" "))
          new ExternalProcessFactory[(Int,Record),IndexedRecord,MonitorResponse,MonitorResponse] {
            override def createPre[T,UIN >: IndexedRecord](): Processor[Either[(Int, Record),T], Either[UIN,T]] = new StatelessProcessor[Either[(Int, Record),T],Either[IndexedRecord,T]] {
              override def process(in: Either[(Int, Record),T], f: Either[IndexedRecord,T]=> Unit): Unit = f(
                in match {
                case Left(t) => Left(new IndexedRecord(t))
                case Right(r) => Right(r)

              })
              override def terminate(f: Either[IndexedRecord,T] => Unit): Unit = ()
            }

            override def createProc[UIN >: IndexedRecord](): ExternalProcess[UIN, MonitorResponse] = new DirectProcess[IndexedRecord,MonitorResponse](command) {
              override def parseLine(line: String): MonitorResponse = VerdictItem(line)

              override def isEndMarker(t: MonitorResponse): Boolean = t.in.equals("")

              override val SYNC_BARRIER_IN: IndexedRecord = (-1,Record(-1l, "", emptyTuple, "SYNC", ""))
              override val SYNC_BARRIER_OUT: MonitorResponse => Boolean = _.in == "SYNC"
            }

            override def createPost(): Processor[MonitorResponse, MonitorResponse] = StatelessProcessor.identity[MonitorResponse]
          }
        }
        case c@_ => fail("Cannot parse the monitor command: {}", c)
      }

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

      //env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
      env.setMaxParallelism(1)
      env.setParallelism(1)

      val monitor = new StreamMonitorBuilder(env)

      // textStream = env.addSource[String](helpers.createStringConsumerForTopic("flink_input","localhost:9092","flink"))
      val textStream = in match {
        case Some(Left((h, p))) => monitor.socketSource(h, p)
        case Some(Right(f)) => if (watchInput) monitor.fileWatchSource(f) else monitor.simpleFileSource(f)
        case _ => fail("Cannot parse the input argument")
      }

      val dfms = if(params.has("decider"))
        Some(new DeciderFlatMapSimple(slicer.degree, formula, params.getInt("windowsize",100)))
      else
        None
      val verdicts = monitor.assemble(textStream, inputFormat, dfms, slicer, processFactory, queueSize)

      out match {
        case Some(Left((h, p))) => monitor.socketSink(verdicts, h, p)
        case Some(Right(f)) => monitor.fileSink(verdicts, f)
        case _ => monitor.printSink(verdicts)
      }

      Rescaler.create(jobName, "127.0.0.1", processors)
      Thread.sleep(2000)
      env.execute(jobName)
    }
  }

}
