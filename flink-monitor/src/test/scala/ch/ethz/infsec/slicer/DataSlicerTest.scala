package ch.ethz.infsec.slicer

import ch.ethz.infsec.TestHelpers
import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.policy.{GenFormula, _}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable

class DataSlicerTest extends AnyFunSuite with Matchers {

  class TestDataSlicer extends DataSlicer {

    override val formula: Formula = GenFormula.resolve(
      All("b", And(Pred("A", Var("b"), Var("x")), Pred("B", Var("y"), Var("b")))))
    override val maxDegree = 4
    override def degree = 4

    override def addSlicesOfValuation(valuation: Array[Any], slices: mutable.HashSet[Int]): Unit =
      if (valuation(0) != null)
        slices ++= List(0, valuation(0).asInstanceOf[Long].toInt)
      else
        slices ++= List(1, valuation(1).asInstanceOf[Long].toInt)

    override val requiresFilter: Boolean = false

    override def filterVerdict(slice: Int, verdict: Fact): Boolean = true

    override def stringify: String = ""

    override def unstringify(s: String): Unit = ()
  }

  test("apply") {
    val records = List(
      Fact.make("A", 101L, Long.box(1), Long.box(2)),
      Fact.make("B", 101L, Long.box(3), Long.box(4)),
      Fact.terminator(101L),
      Fact.terminator(102L),
      Fact.make("A", 103L, Long.box(1), Long.box(2)),
      Fact.make("B", 103L, Long.box(2), Long.box(1)),
      Fact.make("A", 103L, Long.box(2), Long.box(3)),
      Fact.make("B", 103L, Long.box(2), Long.box(3)),
      Fact.terminator(103L),
      Fact.terminator(104L)
    )

    val slicer = new TestDataSlicer()

    val slices = TestHelpers.flatMapAll(slicer, records)
    slices should contain theSameElementsInOrderAs List(
      (0, Fact.make("A", 101L, Long.box(1), Long.box(2))),
      (2, Fact.make("A", 101L, Long.box(1), Long.box(2))),
      (1, Fact.make("B", 101L, Long.box(3), Long.box(4))),
      (3, Fact.make("B", 101L, Long.box(3), Long.box(4))),
      (0, Fact.terminator(101L)),
      (1, Fact.terminator(101L)),
      (2, Fact.terminator(101L)),
      (3, Fact.terminator(101L)),

      (0, Fact.terminator(102L)),
      (1, Fact.terminator(102L)),
      (2, Fact.terminator(102L)),
      (3, Fact.terminator(102L)),

      (0, Fact.make("A", 103L, Long.box(1), Long.box(2))),
      (2, Fact.make("A", 103L, Long.box(1), Long.box(2))),
      (1, Fact.make("B", 103L, Long.box(2), Long.box(1))),
      (2, Fact.make("B", 103L, Long.box(2), Long.box(1))),
      (0, Fact.make("A", 103L, Long.box(2), Long.box(3))),
      (3, Fact.make("A", 103L, Long.box(2), Long.box(3))),
      (1, Fact.make("B", 103L, Long.box(2), Long.box(3))),
      (2, Fact.make("B", 103L, Long.box(2), Long.box(3))),
      (0, Fact.terminator(103L)),
      (1, Fact.terminator(103L)),
      (2, Fact.terminator(103L)),
      (3, Fact.terminator(103L)),

      (0, Fact.terminator(104L)),
      (1, Fact.terminator(104L)),
      (2, Fact.terminator(104L)),
      (3, Fact.terminator(104L))
    )
  }

}
