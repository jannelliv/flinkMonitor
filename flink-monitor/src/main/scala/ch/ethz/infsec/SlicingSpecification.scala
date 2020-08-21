package ch.ethz.infsec

import java.io.PrintWriter

import ch.ethz.infsec.autobalancer.Helpers.Domain
import ch.ethz.infsec.autobalancer.{ConstantHistogram, StatsHistogram}
import ch.ethz.infsec.policy.Formula
import ch.ethz.infsec.slicer.HypercubeSlicer
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
    val rates = SlicingSpecification.parseKeyValueParameter(ratesSpec, _.toDouble)
    if (ratesSpec.nonEmpty && sharesSpec.nonEmpty) {
      throw new IllegalArgumentException("At most one of shares and rates may be given")
    }

    val heavyName = parameters.get("heavy", "")
    val heavyHittersMap = new mutable.HashMap[(String, Int), mutable.HashSet[Any]]()
      .withDefaultValue(new mutable.HashSet[Any]())
    if (heavyName.nonEmpty) {
      logger.info("Reading heavy hitters from file {}", heavyName)
      val heavySource = Source.fromFile(heavyName)
      try {
        for (line <- heavySource.getLines()) {
          val fields = line.split(",", 3)
          val these = heavyHittersMap.getOrElseUpdate((fields(0), fields(1).toInt), new mutable.HashSet[Any]())
          val value: Any = if (fields(2).startsWith("\""))
            fields(2).stripPrefix("\"").stripSuffix("\"")
          else
            Long.box(fields(2).toLong)
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
        formula, degree, new StatsHistogram {
          override def relationSize(relation: String): Double = rates(relation)

          override def merge(statsHistogram: StatsHistogram): Unit = throw new Exception("cannot call merge on this")

          override def heavyHitter(relation: String, attribute: Int): Set[Domain] = heavyHittersMap((relation, attribute)).toSet
        })
    } else {
      logger.info("Optimizing shares for equal statistics and no heavy hitters")
      HypercubeSlicer.optimize(formula, degree, ConstantHistogram())
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
