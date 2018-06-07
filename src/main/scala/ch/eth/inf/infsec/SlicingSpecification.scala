package ch.eth.inf.infsec

import java.io.PrintWriter

import ch.eth.inf.infsec.policy.Formula
import ch.eth.inf.infsec.slicer.HypercubeSlicer
import ch.eth.inf.infsec.trace.{Domain, IntegralValue, StringValue}
import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.io.Source

object SlicingSpecification {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def parseKeyValueParameter[T](arg: String, parseValue: String => T): Map[String, T] =
    arg.split(',').filter(_.nonEmpty).map { x =>
      val xs = x.split("=", 2)
      (xs(0), parseValue(xs(1)))
    }(collection.breakOut)

  def mkSlicer(parameters: ParameterTool, formula: Formula, degree: Int): HypercubeSlicer = {
    val sharesSpec = parameters.get("shares", "")
    val shares = SlicingSpecification.parseKeyValueParameter(sharesSpec, _.toInt)
    if (sharesSpec.nonEmpty) {
      val totalShares = shares.values.product
      if (totalShares > degree) throw new IllegalArgumentException("Total shares exceed degree")
    }

    val ratesSpec = parameters.get("rates", "")
    val rates = SlicingSpecification.parseKeyValueParameter(ratesSpec, _.toLong)
    if (ratesSpec.nonEmpty && sharesSpec.nonEmpty) {
      throw new IllegalArgumentException("At most one of shares and rates may be given")
    }

    val heavyName = parameters.get("heavy", "")
    val heavyHittersMap = new mutable.HashMap[(String, Int), mutable.HashSet[Domain]]()
      .withDefaultValue(new mutable.HashSet[Domain]())
    if (heavyName.nonEmpty) {
      logger.info("Reading heavy hitters from file {}", heavyName)
      val heavySource = Source.fromFile(heavyName)
      try {
        for (line <- heavySource.getLines()) {
          val fields = line.split(",", 3)
          val these = heavyHittersMap.getOrElseUpdate((fields(0), fields(1).toInt), new mutable.HashSet[Domain]())
          val value: Domain = if (fields(2).startsWith("\""))
            StringValue(fields(2).stripPrefix("\"").stripSuffix("\""))
          else
            IntegralValue(fields(2).toLong)
          these += value
        }
      } finally {
        heavySource.close()
      }
    }

    val dataSlicer = if (sharesSpec.nonEmpty) {
      val resolvedShares = shares.map { case (k, e) => (formula.freeVariables.find(_.nameHint == k).get, e) }
      logger.info("Slicing with fixed shares: {}", resolvedShares.mkString(", "))
      HypercubeSlicer.fromSimpleShares(formula, resolvedShares)
    } else if (ratesSpec.nonEmpty) {
      logger.info("Optimizing shares for statistics: {}", rates.mkString(", "))
      HypercubeSlicer.optimize(
        formula, StreamMonitoring.floorLog2(degree), new slicer.Statistics {
          override def relationSize(relation: String): Double = rates(relation)

          override def heavyHitters(relation: String, attribute: Int): Set[Domain] =
            heavyHittersMap((relation, attribute)).toSet
        })
    } else {
      logger.info("Optimizing shares for equal statistics and no heavy hitters")
      HypercubeSlicer.optimize(formula, StreamMonitoring.floorLog2(degree), slicer.Statistics.constant)
    }

    val dumpFilename = parameters.get("dump-shares", "")
    if (dumpFilename.nonEmpty) {
      val variables: Map[Int, String] =
        formula.freeVariables.map(v => (v.freeID, v.nameHint))(collection.breakOut)

      logger.info("Dumping slicer configuration to {}", dumpFilename)
      val dump = new PrintWriter(dumpFilename)
      try {
        def enumerate(i: Int, mask: Int, heavy: List[String]): Unit = if (i < variables.size) {
          val bit = dataSlicer.heavy(i)._1
          enumerate(i + 1, mask, heavy)
          if (bit >= 0)
            enumerate(i + 1, mask | (1 << bit), variables(i) :: heavy)
        } else {
          dump.println("Heavy hitters: " + (if (heavy.isEmpty) "(none)" else heavy.reverse.mkString(", ")))
          val shares = dataSlicer.shares(mask)
          for (v <- 0 until variables.size)
            dump.println(variables(v) + " = " + shares(v))
          dump.println()
        }

        enumerate(0, 0, Nil)
      } finally {
        dump.close()
      }
    }

    dataSlicer
  }
}
