package ch.eth.inf.infsec.analysis

import ch.eth.inf.infsec.policy.{Formula, GenFormula, Policy}
import ch.eth.inf.infsec.slicer.{DataSlicer, HypercubeSlicer}
import ch.eth.inf.infsec.trace._
import ch.eth.inf.infsec.{ProcessorFunction, StreamMonitoring, policy, slicer}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source

object OfflineEvaluation {
  def parseKeyValueParameter[T](arg: String, parseValue: String => T): Map[String, T] =
    arg.split(',').filter(_.nonEmpty).map { x =>
      val xs = x.split("=", 2)
      (xs(0), parseValue(xs(1)))
    }(collection.breakOut)

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
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

    logger.info("Collecting statistics for {} parallel monitors with window size {}", degree, windowSize)

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

    val sharesSpec = parameters.get("shares", "")
    val shares = parseKeyValueParameter(sharesSpec, _.toInt)
    if (sharesSpec.nonEmpty) {
      if (monitoringFormula.isEmpty) throw new IllegalArgumentException("Formula required for slicing")
      val totalShares = shares.values.product
      if (totalShares > degree) throw new IllegalArgumentException("Total shares exceed degree")
    }

    val ratesSpec = parameters.get("rates", "")
    val rates = parseKeyValueParameter(ratesSpec, _.toLong)
    if (ratesSpec.nonEmpty) {
      if (monitoringFormula.isEmpty) throw new IllegalArgumentException("Formula required for slicing")
      if (sharesSpec.nonEmpty) throw new IllegalArgumentException("At most one of shares and rates may be given")
    }

    val heavyName = Option(parameters.get("heavy"))
    val heavyHittersMap = new mutable.HashMap[(String, Int), mutable.HashSet[Domain]]()
      .withDefaultValue(new mutable.HashSet[Domain]())
    for (theHeavyName <- heavyName) {
      logger.info("Reading heavy hitters from file {}", theHeavyName)
      val heavySource = Source.fromFile(theHeavyName)
      try {
        for (line <- heavySource.getLines()) {
          val fields = line.split(",", 3)
          val these = heavyHittersMap.getOrElseUpdate((fields(0), fields(1).toInt), new mutable.HashSet[Domain]())
          these += fields(2)
        }
      } finally {
        heavySource.close()
      }
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // TODO(JS): This skips the last event, because we have to flush the parser once the end of the file is reached.
    val eventStream = env.readTextFile(logFileName)
      .flatMap(new ProcessorFunction(format.createParser()))
      .assignAscendingTimestamps(_.timestamp * 1000)

    val resultStream = if (ratesSpec.nonEmpty) {
      logger.info("Optimizing hypercube for statistics: {}", rates.mkString(", "))
      val dataSlicer = HypercubeSlicer.optimize(
        monitoringFormula.get, StreamMonitoring.floorLog2(degree), new slicer.Statistics {
          override def relationSize(relation: String): Double = rates(relation)

          override def heavyHitters(relation: String, attribute: Int): Set[Domain] =
            heavyHittersMap((relation, attribute)).toSet
        })
      logger.info("Slicing with optimized shares")
      logger.info("Number of configurations: {}", dataSlicer.shares.length)
      logger.info("Shares assuming no heavy hitters: {}", dataSlicer.shares(0).mkString(", "))

      val slicedStream = eventStream.flatMap(new ProcessorFunction(dataSlicer))
      TraceStatistics.analyzeSlices(slicedStream, windowSize, 1)
        .mapWith { case (startTime, (slice, relation), count) =>
          s"${startTime / 1000},$slice,$relation,$count"
        }
    } else if (sharesSpec.nonEmpty) {
      val resolvedShares = shares.map { case (k, e) =>
        (monitoringFormula.get.freeVariables.find(_.nameHint == k).get, e)
      }
      logger.info("Slicing with fixed shares: {}", resolvedShares.mkString(", "))
      val dataSlicer = HypercubeSlicer.fromSimpleShares(monitoringFormula.get, resolvedShares)

      val slicedStream = eventStream.flatMap(new ProcessorFunction(dataSlicer))
      TraceStatistics.analyzeSlices(slicedStream, windowSize, 1)
        .mapWith { case (startTime, (slice, relation), count) =>
          s"${startTime / 1000},$slice,$relation,$count"
        }
    } else if (monitoringFormula.nonEmpty) {
      logger.info("Collecting trace statistics with filtering")
      val dataSlicer = new DataSlicer with Serializable {
        override def addSlicesOfValuation(valuation: Array[Domain], slices: mutable.HashSet[Int]): Unit = slices += 0

        override val remapper: PartialFunction[Int, Int] = PartialFunction(identity)
        override val degree: Int = 1
        override val formula: Formula = monitoringFormula.get
      }

      val filteredStream = eventStream.flatMap(new ProcessorFunction(dataSlicer)).map(_._2)
      TraceStatistics.analyzeRelations(filteredStream, windowSize, 1, degree)
        .mapWith { case (startTime, relation, stats) =>
          val heavyHitters = stats.heavyHitters(degree).map(_.mkString(",")).mkString(";")
          s"${startTime / 1000},$relation,${stats.records};$heavyHitters"
        }
    } else {
      logger.info("Collecting trace statistics without filtering")
      TraceStatistics.analyzeRelations(eventStream, windowSize, 1, degree)
        .mapWith { case (startTime, relation, stats) =>
          val heavyHitters = stats.heavyHitters(degree).map(_.mkString(",")).mkString(";")
          s"${startTime / 1000},$relation,${stats.records};$heavyHitters"
        }
    }

    resultStream.writeAsText(outputFileName)
    env.execute("Offline evaluation")
  }
}
