package ch.ethz.infsec.slicer

import ch.ethz.infsec.TestHelpers
import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.policy.{GenFormula, _}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable

class DataSlicerTest extends FunSuite with Matchers {

  class TestDataSlicer extends DataSlicer {

    override val formula: Formula = GenFormula.resolve(
      All("b", And(Pred("A", Var("b"), Var("x")), Pred("B", Var("y"), Var("b")))))
    override val maxDegree = 4
    override val degree = 4

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
      Fact.make("A", "101", Long.box(1), Long.box(2)),
      Fact.make("B", "101", Long.box(3), Long.box(4)),
      Fact.terminator("101"),
      Fact.terminator("102"),
      Fact.make("A", "103", Long.box(1), Long.box(2)),
      Fact.make("B", "103", Long.box(2), Long.box(1)),
      Fact.make("A", "103", Long.box(2), Long.box(3)),
      Fact.make("B", "103", Long.box(2), Long.box(3)),
      Fact.terminator("103"),
      Fact.terminator("104")
    )

    val slicer = new TestDataSlicer()

    val slices = TestHelpers.flatMapAll(slicer, records)
    slices should contain theSameElementsInOrderAs List(
      (0, Fact.make("A", "101", Long.box(1), Long.box(2))),
      (2, Fact.make("A", "101", Long.box(1), Long.box(2))),
      (1, Fact.make("B", "101", Long.box(3), Long.box(4))),
      (3, Fact.make("B", "101", Long.box(3), Long.box(4))),
      (0, Fact.terminator("101")),
      (1, Fact.terminator("101")),
      (2, Fact.terminator("101")),
      (3, Fact.terminator("101")),

      (0, Fact.terminator("102")),
      (1, Fact.terminator("102")),
      (2, Fact.terminator("102")),
      (3, Fact.terminator("102")),

      (0, Fact.make("A", "103", Long.box(1), Long.box(2))),
      (2, Fact.make("A", "103", Long.box(1), Long.box(2))),
      (1, Fact.make("B", "103", Long.box(2), Long.box(1))),
      (2, Fact.make("B", "103", Long.box(2), Long.box(1))),
      (0, Fact.make("A", "103", Long.box(2), Long.box(3))),
      (3, Fact.make("A", "103", Long.box(2), Long.box(3))),
      (1, Fact.make("B", "103", Long.box(2), Long.box(3))),
      (2, Fact.make("B", "103", Long.box(2), Long.box(3))),
      (0, Fact.terminator("103")),
      (1, Fact.terminator("103")),
      (2, Fact.terminator("103")),
      (3, Fact.terminator("103")),

      (0, Fact.terminator("104")),
      (1, Fact.terminator("104")),
      (2, Fact.terminator("104")),
      (3, Fact.terminator("104"))
    )
  }

}
