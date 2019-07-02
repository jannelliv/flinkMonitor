package ch.ethz.infsec

import java.io._
import java.lang.ProcessBuilder.Redirect
import java.util.concurrent.{Callable, Executors, TimeUnit}

import ch.ethz.infsec.analysis.TraceAnalysis
import ch.ethz.infsec.monitor._
import ch.ethz.infsec.policy.{Formula, Policy}
import ch.ethz.infsec.tools.Rescaler
import ch.ethz.infsec.tools.Rescaler.RescaleInitiator
import ch.ethz.infsec.trace.{CsvFormat, MonpolyFormat, TraceMonitor, _}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.IdPartitioner
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SocketClientSink}
import org.apache.flink.streaming.api.functions.source.{FileProcessingMode, SocketTextStreamFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
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
  var markDatabaseEnd: Boolean = false
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
    markDatabaseEnd = params.getBoolean("mark-database-end", false)

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

  def exename(str: String):String = {
    val ss = str.split('/')
    ss(ss.length-1)
  }


  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)

    val analysis = params.getBoolean("analysis", false)

    if (analysis) TraceAnalysis.prepareSimulation(params)
    else {
      init(params)

      if (formula.freeVariables.isEmpty) { fail("Closed formula cannot be sliced") }
      val slicer = SlicingSpecification.mkSlicer(params, formula, processors)


      val processFactory:ExternalProcessFactory[(Int, Record), MonitorRequest, String, String] = monitorCommand match {
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
          new ExternalProcessFactory[(Int,Record),IndexedRecord,String,String] {
            override def createPre[T,UIN >: IndexedRecord](): Processor[Either[(Int, Record),T], Either[UIN,T]] = new StatelessProcessor[Either[(Int, Record),T],Either[IndexedRecord,T]] {
              override def process(in: Either[(Int, Record),T], f: Either[IndexedRecord,T]=> Unit): Unit = f(
                in match {
                case Left(t) => Left(new IndexedRecord(t))
                case Right(r) => Right(r)

              })
              override def terminate(f: Either[IndexedRecord,T] => Unit): Unit = ()
            }

            override def createProc[UIN >: IndexedRecord](): ExternalProcess[UIN, String] = new DirectProcess[IndexedRecord,String](command) {
              override def parseLine(line: String): String = line

              override def isEndMarker(t: String): Boolean = t.equals("")

              override val SYNC_BARRIER_IN: IndexedRecord = (-1,Record(-1l, "", emptyTuple, "SYNC", ""))
              override val SYNC_BARRIER_OUT: String => Boolean = _ == "SYNC"
            }

            override def createPost(): Processor[String, String] = StatelessProcessor.identity[String]
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
        case _ => fail("Cannot parse the input argument")
      }

      val parsedTrace = textStream.flatMap(new ProcessorFunction(new TraceMonitor(inputFormat.createParser(), new RescaleInitiator().rescale)))
        .name("Parser")
        .uid("input-parser")
        .setMaxParallelism(1)
        .setParallelism(1)

      val slicedTrace = parsedTrace
        .flatMap(new ProcessorFunction(slicer)).name("Slicer").uid("slicer")
        .setMaxParallelism(1)
        .setParallelism(1)
        .partitionCustom(new IdPartitioner, 0)

      // Parallel node
      // TODO(JS): Timeout? Capacity?
      // TODO(JS): Set parallelism to slicer.degree once elastic rescaling is implemented.
      val verdicts = ExternalProcessOperator.transform[(Int, Record), MonitorRequest, String, String](
        slicer,
        slicedTrace,
        processFactory,
        queueSize).setParallelism(processors).setMaxParallelism(processors).name("Monitor").uid("monitor")

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
            new BucketingSink[String](f))
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
