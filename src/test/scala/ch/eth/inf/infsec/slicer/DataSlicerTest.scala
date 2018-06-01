package ch.eth.inf.infsec
package slicer

import ch.eth.inf.infsec.policy._
import ch.eth.inf.infsec.trace.{Domain, IntegralValue, Record, Tuple}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.{TraversableOnce, mutable}

class DataSlicerTest extends FunSuite with Matchers {

  class TestDataSlicer extends DataSlicer {

    override val formula: Formula = GenFormula.resolve(
      All("b", And(Pred("A", Var("b"), Var("x")), Pred("B", Var("y"), Var("b")))))
    override val degree = 4

    override def addSlicesOfValuation(valuation: Array[Domain], slices: mutable.HashSet[Int]): Unit =
      if (valuation(0) != null)
        slices ++= List(0, valuation(0).asInstanceOf[IntegralValue].value.toInt)
      else
        slices ++= List(1, valuation(1).asInstanceOf[IntegralValue].value.toInt)
  }

  test("apply") {
    val records = List(
      Record(101, "A", Tuple(1, 2)),
      Record(101, "B", Tuple(3, 4)),
      Record.markEnd(101),
      Record.markEnd(102),
      Record(103, "A", Tuple(1, 2)),
      Record(103, "B", Tuple(2, 1)),
      Record(103, "A", Tuple(2, 3)),
      Record(103, "B", Tuple(2, 3)),
      Record.markEnd(103),
      Record.markEnd(104)
    )

    val slicer = new TestDataSlicer()
    implicit val type1 = TypeInfo[Record]()
    implicit val type2 = TypeInfo[(Int, Record)]()

    //val slices = slicer(events).seq
    val slices = slicer.processAll(records)
    slices should contain theSameElementsInOrderAs List(
      (0, Record(101, "A", Tuple(1, 2))),
      (2, Record(101, "A", Tuple(1, 2))),
      (1, Record(101, "B", Tuple(3, 4))),
      (3, Record(101, "B", Tuple(3, 4))),
      (0, Record.markEnd(101)),
      (1, Record.markEnd(101)),
      (2, Record.markEnd(101)),
      (3, Record.markEnd(101)),

      (0, Record.markEnd(102)),
      (1, Record.markEnd(102)),
      (2, Record.markEnd(102)),
      (3, Record.markEnd(102)),

      (0, Record(103, "A", Tuple(1, 2))),
      (2, Record(103, "A", Tuple(1, 2))),
      (1, Record(103, "B", Tuple(2, 1))),
      (2, Record(103, "B", Tuple(2, 1))),
      (0, Record(103, "A", Tuple(2, 3))),
      (3, Record(103, "A", Tuple(2, 3))),
      (1, Record(103, "B", Tuple(2, 3))),
      (2, Record(103, "B", Tuple(2, 3))),
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
