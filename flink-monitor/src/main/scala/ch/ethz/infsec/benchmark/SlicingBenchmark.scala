package ch.ethz.infsec.benchmark

import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.policy.{GenFormula, _}
import ch.ethz.infsec.slicer.HypercubeSlicer
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object SlicingBenchmark {
  val random = new Random(1234)

  def mkRecord(): Fact = Fact.make("P", 10203L,
    Long.box(random.nextInt().toLong),
    Long.box(random.nextInt().toLong),
    Long.box(random.nextInt().toLong),
    Long.box(random.nextInt().toLong))

  def main(args: Array[String]): Unit = {
    val formula = GenFormula.resolve(Or(
      Pred("P", Var("a"), Var("b"), Var("c"), Var("d")),
      Pred("P", Var("a"), Var("b"), Var("c"), Var("e"))))
    val slicer = HypercubeSlicer.fromSimpleShares(formula, formula.freeVariables.map(v => v -> 4)(collection.breakOut))
    val collector = new Collector[(Int, Fact)] {
      val buffer = new ArrayBuffer[(Int, Fact)]()

      override def collect(t: (Int, Fact)): Unit = buffer += t

      override def close(): Unit = ()
    }

    val runner = new BenchmarkRunner(batchSize = 1000, reportInterval = 1000, warmupTime = 3000)
    runner.measure(() => {
      collector.buffer.clear()
      slicer.flatMap(mkRecord(), collector)
      (collector, 0)
    })
  }
}
