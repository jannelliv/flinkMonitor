package ch.eth.inf.infsec

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.lang.ProcessBuilder.Redirect
import java.util.concurrent.LinkedBlockingQueue

import ch.eth.inf.infsec.trace.{Event, MonpolyFormat}
import ch.eth.inf.infsec.policy.{Formula, Policy}
import ch.eth.inf.infsec.slicer.{HypercubeSlicer, Slicer, Statistics}
import org.apache.flink.api.java.functions.IdPartitioner
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.log4j.Logger

import scala.collection.JavaConversions
import scala.io.Source


object StreamMonitoring {

  val logger = Logger.getLogger(this.getClass)

  var in:Option[Either[(String,Int),String]] = _
  var out:Option[Either[(String,Int),String]] = _

  var processorExp: Int = 0
  var processors:Int=0

  var monitorCommand: String = ""
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

  def parseArgs(ss:String): Option[Either[(String,Int),String]] = {
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

  def init(params:ParameterTool) {

    in = parseArgs(params.get("in","127.0.0.1:9000"))
    out = parseArgs(params.get("out",""))

    val requestedProcessors = params.getInt("processors",1)
    processorExp = floorLog2(requestedProcessors).max(0)
    processors = 1 << processorExp
    if (processors != requestedProcessors) {
      logger.warn(s"Number of processors is not a power of two, using $processors instead")
    }
    logger.info(s"Using $processors parallel monitors")

    monitorCommand = params.get("monitor", "monpoly")
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

  def mkSlicer(): Slicer = {
    // TODO(JS): Get statistics from somewhere
    logger.info("Optimizing slicer ...")
    val slicer = HypercubeSlicer.optimize(formula, processorExp, Statistics.constant)
    logger.info(s"Selected shares: ${slicer.shares.mkString(", ")}")
    slicer
  }

  def main(args: Array[String]) {

    val params = ParameterTool.fromArgs(args)
    init(params)
    val slicer = mkSlicer()
    val monitorArgs = List(monitorCommand, "-sig", signatureFile, "-formula", formulaFile, "-negate")

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    //Single node
    val textStream  = in match {
      case Some(Left((h,p))) =>  env.socketTextStream(h, p)
      case Some(Right(f)) =>  env.readTextFile(f)
      case _ => logger.error("Cannot parse the input argument"); sys.exit(1)
    }
    val parsedTrace = textStream.map(MonpolyFormat.parseLine _).filter(_.isDefined).map(_.get)
    val slicedTrace = parsedTrace.flatMap(slicer(_)).partitionCustom(new IdPartitioner(),0).setParallelism(processors).keyBy(e=>e._1)

    //Parallel nodes
    val verdicts = slicedTrace.process[String](new MonitorFunction(monitorArgs)).setParallelism(processors).map(_+"\n")

    //Single node

    out match {
      case Some(Left((h,p))) =>  verdicts.writeToSocket(h,p,new SimpleStringSchema()).setParallelism(1)
      case Some(Right(f)) =>  verdicts.writeAsText(f).setParallelism(1)
      case _ => verdicts.print().setParallelism(1)
    }
    env.execute("Parallel Online Monitor")
  }

}

// FIXME(JS): This doesn't work, because all Flink operators must have serializable state.
class MonitorFunction(val command: Seq[String]) extends ProcessFunction[(Int, Event), String] {

  private val inputQueue = new LinkedBlockingQueue[Option[Event]]()
  private val outputQueue = new LinkedBlockingQueue[String]()

  private var process: Process = _
  private var inputWorker: Thread = _
  private var outputWorker: Thread = _

  private val TIMEOUT = 500
  // TODO(JS): Logging, error handling, clean-up etc.
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    process = new ProcessBuilder(JavaConversions.seqAsJavaList(command))
      .redirectError(Redirect.INHERIT)
      .start()


    try {
      val f = process.getClass().getDeclaredField("pid");
      f.setAccessible(true);
      val pid = f.getInt(process)
      println("MONITOR PID: " + pid)

    } catch {
      case _:Exception =>
    }

    val input = new BufferedWriter(new OutputStreamWriter(process.getOutputStream))
    val output = new BufferedReader(new InputStreamReader(process.getInputStream))

    inputWorker = new Thread {

      val logger = Logger.getLogger(this.getClass)

      override def run(): Unit = {
        try {
          var running = true
          while (running) {
            inputQueue.take() match {
              case Some(event: Event) =>
                input.write(MonpolyFormat.printEvent(event))
                input.flush()
                logger.debug(s"Monitor ${this.hashCode()} - IN: ${event.toString}")
              case None => running = false
            }
          }
        } finally {
          input.close()
        }
      }
    }
    inputWorker.start()

    outputWorker = new Thread {
      override def run(): Unit = {
        try {
          var running = true
          do {
            val line = output.readLine()
            if (line == null)
              running = false
            else {
              outputQueue.put(line)
            }
          } while (running)
        } finally {
          output.close()
        }
      }
    }
    outputWorker.start()
  }

  override def close(): Unit = {
    if (inputWorker != null) {
      inputQueue.add(None)
      inputWorker.join()
    }
    if (outputWorker != null)
      outputWorker.join()
    if (process != null)
      process.waitFor()

    super.close()
  }


    override def processElement(in: (Int, Event),
      context: ProcessFunction[(Int, Event), String]#Context,
      collector: Collector[String]): Unit =
  {


      context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime()+TIMEOUT)

      inputQueue.put(Some(in._2))

      // FIXME(JS): Verdicts may be delayed for an infinite amount of time if there are no new events to process.
      // Use a timer?
      var moreVerdicts = true
      do {
        val verdict = outputQueue.poll()
        if (verdict == null)
          moreVerdicts = false
        else
          collector.collect(verdict)
      } while (moreVerdicts)

  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[(Int, Event), String]#OnTimerContext, collector: Collector[String]): Unit = {
    var moreVerdicts = true
    do {
      val verdict = outputQueue.poll()
      if (verdict == null)
        moreVerdicts = false
      else
        collector.collect(verdict)
    } while (moreVerdicts)
  }
}

//val verdicts = slicedTrace.reduce(monpoly)

//Type issue example with sized
//    val r:Relation = Set(Sized(SInteger(1),SString("a")),
//                         Sized(SInteger(2),SString("b")))
//    val e:Event = (4,Set(r))



