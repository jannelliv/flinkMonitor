package ch.ethz.infsec.slicer

import ch.ethz.infsec.policy._
import ch.ethz.infsec.trace.{EventRecord, Record, Tuple}
import ch.ethz.infsec.monitor.{Domain, IntegralValue}
import ch.ethz.infsec.policy.GenFormula
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable

class DataSlicerTest extends FunSuite with Matchers {

  class TestDataSlicer extends DataSlicer {

    override val formula: Formula = GenFormula.resolve(
      All("b", And(Pred("A", Var("b"), Var("x")), Pred("B", Var("y"), Var("b")))))
    override val degree = 4

    override def addSlicesOfValuation(valuation: Array[Domain], slices: mutable.HashSet[Int]): Unit =
      if (valuation(0) != null)
        slices ++= List(0, valuation(0).getIntegralVal.get.toInt)
      else
        slices ++= List(1, valuation(1).getIntegralVal.get.toInt)


    override def getState(): Array[Byte] = Array.emptyByteArray

    override def restoreState(state: Option[Array[Byte]]): Unit = {}

    override def mkVerdictFilter(slice: Int)(verdict: Tuple): Boolean = true
  }

  test("apply") {
    val records = List(
      EventRecord(101, "A", Tuple(1, 2)),
      EventRecord(101, "B", Tuple(3, 4)),
      Record.markEnd(101),
      Record.markEnd(102),
      EventRecord(103, "A", Tuple(1, 2)),
      EventRecord(103, "B", Tuple(2, 1)),
      EventRecord(103, "A", Tuple(2, 3)),
      EventRecord(103, "B", Tuple(2, 3)),
      Record.markEnd(103),
      Record.markEnd(104)
    )

    val slicer = new TestDataSlicer()

    val slices = slicer.processAll(records)
    slices should contain theSameElementsInOrderAs List(
      (0, EventRecord(101, "A", Tuple(1, 2))),
      (2, EventRecord(101, "A", Tuple(1, 2))),
      (1, EventRecord(101, "B", Tuple(3, 4))),
      (3, EventRecord(101, "B", Tuple(3, 4))),
      (0, Record.markEnd(101)),
      (1, Record.markEnd(101)),
      (2, Record.markEnd(101)),
      (3, Record.markEnd(101)),

      (0, Record.markEnd(102)),
      (1, Record.markEnd(102)),
      (2, Record.markEnd(102)),
      (3, Record.markEnd(102)),

      (0, EventRecord(103, "A", Tuple(1, 2))),
      (2, EventRecord(103, "A", Tuple(1, 2))),
      (1, EventRecord(103, "B", Tuple(2, 1))),
      (2, EventRecord(103, "B", Tuple(2, 1))),
      (0, EventRecord(103, "A", Tuple(2, 3))),
      (3, EventRecord(103, "A", Tuple(2, 3))),
      (1, EventRecord(103, "B", Tuple(2, 3))),
      (2, EventRecord(103, "B", Tuple(2, 3))),
      (0, Record.markEnd(103)),
      (1, Record.markEnd(103)),
      (2, Record.markEnd(103)),
      (3, Record.markEnd(103)),

      (0, Record.markEnd(104)),
      (1, Record.markEnd(104)),
      (2, Record.markEnd(104)),
      (3, Record.markEnd(104))
    )
  }

}
