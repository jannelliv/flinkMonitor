package ch.ethz.infsec.benchmark

import ch.ethz.infsec.policy._
import ch.ethz.infsec.slicer.HypercubeSlicer
import ch.ethz.infsec.trace.{EventRecord, IntegralValue, Record}
import ch.ethz.infsec.policy.GenFormula

import scala.util.Random

object SlicingBenchmark {
  val random = new Random(1234)

  def mkRecord: Record = EventRecord(10203L, "P",
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