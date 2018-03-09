package ch.eth.inf.infsec.benchmark

import ch.eth.inf.infsec.policy._
import ch.eth.inf.infsec.slicer.HypercubeSlicer
import ch.eth.inf.infsec.trace.{IntegralValue, Record}

import scala.util.Random

object SlicingBenchmark {
  val random = new Random(1234)

  def mkRecord: Record = Record(10203L, "P",
    Array(
      IntegralValue(random.nextInt()),
      IntegralValue(random.nextInt()),
      IntegralValue(random.nextInt()),
      IntegralValue(random.nextInt)))

  def main(args: Array[String]): Unit = {
    val formula = GenFormula.resolve(Or(
      Pred("P", Var("a"), Var("b"), Var("c"), Var("d")),
      Pred("P", Var("a"), Var("b"), Var("c"), Var("e"))))
    val slicer = HypercubeSlicer.fromSimpleShares(formula, formula.freeVariables.map(v => v -> 4)(collection.breakOut))

    val runner = new BenchmarkRunner(batchSize = 1000, reportInterval = 1000, warmupTime = 3000)
    runner.measure(() => (slicer.process(mkRecord, p => ()), 0))
  }
}