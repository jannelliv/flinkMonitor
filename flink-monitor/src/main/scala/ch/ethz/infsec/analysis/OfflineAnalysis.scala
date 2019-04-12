package ch.ethz.infsec.analysis

import ch.ethz.infsec._
import ch.ethz.infsec.policy.{Formula, GenFormula, Policy}
import ch.ethz.infsec.slicer.DataSlicer
import ch.ethz.infsec.monitor.{Domain}
import ch.ethz.infsec.trace.{TraceFormat,MonpolyFormat,CsvFormat,Tuple}
import ch.ethz.infsec.policy.{GenFormula, Policy}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source

object OfflineAnalysis {
  private val logger = LoggerFactory.getLogger(getClass)

  private def heavyHittersWithCountsToString(stats: Statistics): String = {
    val builder = new mutable.StringBuilder()
    var sep1 = ""
    for (heavy <- stats.heavyCounts) {
      builder.append(sep1)
      sep1 = ";"
      var sep2 = ""
      for ((value, count) <- heavy) {
        builder.append(sep2).append(value).append(',').append(count)
        sep2 = ","
      }
    }
    builder.toString()
  }

  def main(args: Array[String]): Unit = {
    val parameters = ParameterTool.fromArgs(args)

    val logFileName = parameters.getRequired("log")
    val outputFileName = parameters.getRequired("out")

    val formatName = parameters.get("format", "monpoly")
    val format: TraceFormat = formatName match {
      case "monpoly" => MonpolyFormat
      case "csv" => CsvFormat
      case name => throw new IllegalArgumentException(s"Unknown log format: $name")
    }

    logger.info("Analysing {} log file {}", formatName, logFileName: Any)
    logger.info("Writing statistics to {}", outputFileName)

    val windowSize = parameters.getInt("window", 60)
    if (windowSize < 1) throw new IllegalArgumentException("Window size must be at least 1")
    val degree = parameters.getInt("degree", 16)
    if (degree < 1) throw new IllegalArgumentException("Degree must be at least 1")

    logger.info("Statistics for {} parallel monitors with window size {}", degree, windowSize)

    val formulaName = Option(parameters.get("formula"))
    val monitoringFormula = formulaName.map { theFormulaName =>
      logger.info("Reading formula file {}", theFormulaName)
      val formulaSource = Source.fromFile(theFormulaName)
      val policyFormula = try {
        Policy.read(formulaSource.mkString).right.get
      } finally {
        formulaSource.close()
      }
      val monitoringFormula = GenFormula.pushNegation(policy.Not(policyFormula))
      logger.info("Monitored formula: {}", GenFormula.print(monitoringFormula))
      logger.info("Free variables: {}", monitoringFormula.freeVariables.toSeq.sortBy(_.freeID).mkString(", "))
      monitoringFormula
    }

    val slicer = if (parameters.has("rates") || parameters.has("shares"))
      Some(SlicingSpecification.mkSlicer(parameters, monitoringFormula.get, degree))
    else None

    val collectHeavy = parameters.getBoolean("collect-heavy", true)
    val heavyWithCounts = parameters.getBoolean("with-counts", false)
    val minThreshold = parameters.getInt("threshold", 0)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // TODO(JS): This skips the last event, because we have to flush the parser once the end of the file is reached.
    val eventStream = env.readTextFile(logFileName)
      .flatMap(new ProcessorFunction(format.createParser()))
      .assignAscendingTimestamps(_.timestamp * 1000)

    val resultStream = slicer match {
      case Some(theSlicer) =>
        logger.info("Collecting slice statistics")
        val slicedStream = eventStream.flatMap(new ProcessorFunction(theSlicer))
        TraceStatistics.analyzeSlices(slicedStream, windowSize)
          .mapWith { case (startTime, (slice, relation), count) =>
            s"${startTime / 1000},$slice,$relation,$count"
          }

      case None =>
        val filteredStream = if (monitoringFormula.nonEmpty) {
          logger.info("Collecting trace statistics with filtering")
          val dataSlicer = new DataSlicer with Serializable {
            override def addSlicesOfValuation(
              valuation: Array[Domain],
              slices: mutable.HashSet[Int]): Unit = slices += 0

            override val degree: Int = 1
            override val formula: Formula = monitoringFormula.get

            override def getState(): Array[Byte] = Array.emptyByteArray

            override def restoreState(state: Option[Array[Byte]]): Unit = {}

            override def mkVerdictFilter(slice: Int)(verdict: Tuple): Boolean = true
          }
          eventStream.flatMap(new ProcessorFunction(dataSlicer)).map(_._2)
        } else {
          logger.info("Collecting trace statistics without filtering")
          eventStream
        }

        if (collectHeavy) {
          TraceStatistics.analyzeRelations(filteredStream, windowSize, degree, minThreshold)
            .mapWith { case (startTime, relation, stats) =>
              val heavyHitters = if (heavyWithCounts)
                  heavyHittersWithCountsToString(stats)
                else
                  stats.heavyHitters.map(_.mkString(",")).mkString(";")
              s"${startTime / 1000},$relation,${stats.events};$heavyHitters"
            }
        } else {
          TraceStatistics.analyzeRelationFrequencies(filteredStream, windowSize)
            .mapWith { case (startTime, relation, count) => s"${startTime / 1000},$relation,$count" }
        }
    }

    resultStream.writeAsText(outputFileName)
    env.execute("Offline evaluation")
  }
}
