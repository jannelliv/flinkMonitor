package ch.eth.inf.infsec.analysis

import ch.eth.inf.infsec.{StreamMonitoring, policy}
import ch.eth.inf.infsec.policy.{GenFormula, Policy}
import ch.eth.inf.infsec.slicer
import ch.eth.inf.infsec.slicer.HypercubeSlicer
import ch.eth.inf.infsec.trace._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.slf4j.LoggerFactory

import scala.io.Source

object OfflineEvaluation {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    val parameters = ParameterTool.fromArgs(args)

    val logFileName = parameters.getRequired("log")
    val outputFileName = parameters.getRequired("out")
    val formatName = parameters.get("format", "monpoly")
    val windowSize = parameters.getInt("window", 60)
    if (windowSize < 1) throw new IllegalArgumentException("Window size must be at least 1")
    val degree = parameters.getInt("degree", 16)
    if (degree < 1) throw new IllegalArgumentException("Degree must be at least 1")

    val formulaName = Option(parameters.get("formula"))
    val ratesSpec = parameters.get("rates", "")
    val rates: Map[String, Long] = ratesSpec.split(',').filter(_.nonEmpty).map { x =>
      val xs = x.split('=')
      (xs(0), xs(1).toLong)
    }(collection.breakOut)

    val format: TraceFormat = formatName match {
      case "monpoly" => MonpolyFormat
      case "csv" => CsvFormat
      case name => throw new IllegalArgumentException(s"Unknown log format: $name")
    }

    logger.info("Reading {} log file {}", formatName, logFileName: Any)
    logger.info("Writing statistics to {}", outputFileName)
    logger.info("Number of parallel monitors is {}", degree)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // TODO(JS): This skips the last event, because we have to flush the parser once the end of the file is reaced.
    val eventStream = env.readTextFile(logFileName)
      .flatMap(new ParseTraceFunction(format))
      .assignAscendingTimestamps(_.timestamp * 1000)

    val resultStream = formulaName match {
      case None =>
        logger.info("Collecting trace statistics with window size {}", windowSize)
        TraceStatistics.analyzeRelations(eventStream, windowSize, 1, degree)
          .mapWith { case (startTime, relation, stats) =>
            val heavyHitters = stats.heavyHitters(degree).map(_.mkString(",")).mkString(";")
            s"$startTime,$relation,${stats.rates.events},${stats.rates.tuples};$heavyHitters"
          }

      case Some(theFormulaName) =>
        logger.info("Reading formula file {}", theFormulaName)
        val formulaSource = Source.fromFile(theFormulaName)
        val policyFormula = try {
          Policy.read(formulaSource.mkString).right.get
        } finally {
          formulaSource.close()
        }
        val monitoringFormula = GenFormula.pushNegation(policy.Not(policyFormula))

        logger.info("Optimizing formula {}", monitoringFormula)
        logger.info("Statistics: {}", rates.mkString(", "))
        val dataSlicer = HypercubeSlicer.optimize(
          monitoringFormula, StreamMonitoring.floorLog2(degree), new slicer.Statistics {
            override def relationSize(relation: String): Double = rates(relation)
            override def heavyHitters(relation: String, attribute: Int): Set[Any] = Set.empty
          })
        logger.info("Optimization complete")

        logger.info("Collecting slice statistics with window size {}", windowSize)
        val slicedStream = eventStream.flatMap(dataSlicer(_))
        TraceStatistics.analyzeSlices(slicedStream, windowSize, 1)
          .mapWith { case (startTime, (slice, relation), stats) =>
            s"$startTime,$slice,$relation,${stats.events},${stats.tuples}"
          }
    }

    resultStream.writeAsText(outputFileName)
    env.execute("Offline evaluation")
  }
}
