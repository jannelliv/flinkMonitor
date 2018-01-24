package ch.eth.inf.infsec

import org.apache.flink.api.java.utils.ParameterTool
import shapeless.{Fin, Nat, Sized}
import scala.reflect.api.TypeTags

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.lang.ProcessBuilder.Redirect
import java.util.concurrent.LinkedBlockingQueue

import ch.eth.inf.infsec.policy.{Formula, Policy}
import ch.eth.inf.infsec.slicer.{HypercubeSlicer, Slicer, Statistics}
import org.apache.flink.api.java.functions.IdPartitioner
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConversions
import scala.io.Source

object StreamMonitoring {
  var hostName:String=""
  var port:Int=0
  var processorExp: Int = 0
  var processors:Int=0

  var monitorCommand: String = ""
  var formulaFile: String = ""
  var signatureFile: String = ""

  var formula: Formula = policy.False()

  private def floorLog2(x: Int): Int = {
    var remaining = x
    var y = -1
    while (remaining > 0) {
      remaining >>= 1
      y += 1
    }
    y
  }

  def init(params:ParameterTool) {
    hostName = params.get("hostname","127.0.0.1")
    port = params.getInt("port",9000)

    val requestedProcessors = params.getInt("processors",1)
    processorExp = floorLog2(requestedProcessors).max(0)
    processors = 1 << processorExp
    if (processors != requestedProcessors) {
      println(s"Warning: number of processors is not a power of two, using $processors instead")
    }

    monitorCommand = params.get("monitor", "monpoly")
    signatureFile = params.get("sig")

    formulaFile = params.get("formula")
    val formulaSource = Source.fromFile(formulaFile).mkString
    formula = Policy.read(formulaSource) match {
      case Left(err) =>
        println("Error: " + err)
        sys.exit(1)
      case Right(phi) => phi
    }
  }

  def mkSlicer(): Slicer = {
    // TODO(JS): Get statistics from somewhere
    val statistics = new Statistics {
      override def relationSize(relation: String): Double = 1000.0
    }
    println("Optimizing slicer ...")
    val slicer = HypercubeSlicer.optimize(formula, processorExp, statistics)
    println(s"Selected shares: ${slicer.shares.mkString(", ")}")
    slicer
  }

  def mkPartitioner():Criteria[Integer] = {
    new FlinkPartitioner[Integer](new IdPartitioner())
  }

  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)
    init(params)
    val slicer = mkSlicer()
    val partitioner = mkPartitioner()
    val monitorArgs = List(monitorCommand, "-sig", signatureFile, "-formula", formulaFile, "-negate")

    val textStream:Stream[String] = FlinkAdapter.init(hostName, port)

    implicit val type1 = TypeInfo[Option[Event]]()
    implicit val type2 = TypeInfo[Event]()
    implicit val type3 = TypeInfo[(Int,Event)]()


    val parsedTrace:Stream[Event] = textStream.map(parseLine _).filter(_.isDefined).map(_.get)
    val slicedTrace:Stream[(Int,Event)] = slicer.apply(parsedTrace).partition(mkPartitioner(), 0)

    //TODO: temporary broke the abstraction
    val verdicts = slicedTrace.asInstanceOf[FlinkStream[(Int,Event)]].wrapped.process[String](new MonitorFunction(monitorArgs))
    verdicts.print().setParallelism(1)

    FlinkAdapter.execute("Parallel Online Monitor")
  }

}

// FIXME(JS): This doesn't work, because all Flink operators must have serializable state.
class MonitorFunction(val command: Seq[String]) extends ProcessFunction[(Int, Event), String] {

  private val inputQueue = new LinkedBlockingQueue[Option[Event]]()
  private val outputQueue = new LinkedBlockingQueue[String]()

  private var process: Process = _
  private var inputWorker: Thread = _
  private var outputWorker: Thread = _

  // TODO(JS): Logging, error handling, clean-up etc.
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    process = new ProcessBuilder(JavaConversions.seqAsJavaList(command))
      .redirectError(Redirect.INHERIT)
      .start()

    val input = new BufferedWriter(new OutputStreamWriter(process.getOutputStream))
    val output = new BufferedReader(new InputStreamReader(process.getInputStream))

    inputWorker = new Thread {
      override def run(): Unit = {
        try {
          var running = true
          while (running) {
            inputQueue.take() match {
              case Some(event: Event) =>
                input.write(printEvent(event))
                input.flush()
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
            else
              outputQueue.put(line)
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
    inputQueue.put(Some(in._2))

    // FIXME(JS): Verdicts may be delayed for an infinite amount of time if there are no new events to process.
    // Use a timer?
    var moreVerdicts = true
    do {
      var verdict = outputQueue.peek()
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



