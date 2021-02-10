package ch.ethz.infsec.util

import java.io._
import java.lang.ProcessBuilder.Redirect
import java.util.concurrent.{Callable, Executors, TimeUnit}

import ch.ethz.infsec.kafka.MonitorKafkaConfig
import ch.ethz.infsec.monitor._
import ch.ethz.infsec.policy.{Formula, Policy}
import ch.ethz.infsec.trace.parser.{Crv2014CsvParser, MonpolyTraceParser, TraceParser}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.slf4j.LoggerFactory
import org.slf4j.helpers.MessageFormatter
import ch.ethz.infsec.util._

import scala.collection.JavaConverters
import scala.io.Source

object MonitoringAids {

  private val logger = LoggerFactory.getLogger(MonitoringAids.getClass)

  private val MONPOLY_CMD = "monpoly"
  private val ECHO_DEJAVU_CMD = "echo-dejavu"
  private val ECHO_MONPOLY_CMD = "echo-monpoly"
  private val DEJAVU_CMD = "dejavu"

  var jobName: String = ""

  var checkpointUri: String = ""
  var checkpointInterval: Int = 10000
  var restarts: Int = 0

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

  var injectFault = -1
  var simulateKafkaProducer: Boolean = false
  var queueSize = 10000
  var inputParallelism = 1
  var clearTopic = false

  //var formula: Formula = policy.False()

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


    val commandString = params.get("command")
    command = if (commandString == null) Seq.empty else commandString.split(' ')
    signatureFile = params.get("sig")
    formulaFile = params.get("formula")
    val formulaSource = Source.fromFile(formulaFile).mkString
    //formula = Policy.read(formulaSource) match {
     // case Left(err) => fail("Cannot parse the formula: " + err)
      //case Right(phi) => phi
    //}

    initialStateFile = Option(params.get("load"))

    queueSize = params.getInt("queueSize", queueSize)
    injectFault = params.getInt("inject-fault", injectFault)
  }
}
