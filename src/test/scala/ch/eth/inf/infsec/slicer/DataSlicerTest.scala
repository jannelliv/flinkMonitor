package ch.eth.inf.infsec.slicer

import ch.eth.inf.infsec.SeqStream
import org.scalatest.{FunSuite, Matchers}

import scala.collection.TraversableOnce

class DataSlicerTest extends FunSuite with Matchers {

  class TestDataSlicer extends DataSlicer {
    override val formula = Formula(Set(Atom("A", BoundVar(0), FreeVar(0)), Atom("B", FreeVar(1), BoundVar(0))))
    override val degree = 4

    override def slicesOfValuation(valuation: Array[Option[Any]]): TraversableOnce[Int] =
      if (valuation(0).isDefined)
        Array(0, valuation(0).get.asInstanceOf[Int])
      else
        Array(1, valuation(1).get.asInstanceOf[Int])
  }

  test("apply") {
    val events = new SeqStream[Event](Array(
      Event(101, Map(
        "A" -> Array[Tuple](Array(1, 2)),
        "B" -> Array[Tuple](Array(3, 4)))),
      Event(102, Map("B" -> Array[Tuple]())),
      Event(103, Map(
        "A" -> Array[Tuple](Array(1, 2), Array(2, 3)),
        "B" -> Array[Tuple](Array(2, 1), Array(2, 3)))),
      Event(104, Map())
    ))
    val slicer = new TestDataSlicer()
    val slices = slicer(events).seq
    slices should contain inOrderOnly (
      (0, Event(101, Map("A" -> Array[Tuple](Array(1, 2)), "B" -> Array[Tuple]()))),
      (1, Event(101, Map("A" -> Array[Tuple](), "B" -> Array[Tuple](Array(3, 4))))),
      (2, Event(101, Map("A" -> Array[Tuple](Array(1, 2)), "B" -> Array[Tuple]()))),
      (3, Event(101, Map("A" -> Array[Tuple](), "B" -> Array[Tuple](Array(3, 4))))),

      (0, Event(102, Map("B" -> Array[Tuple]()))),
      (1, Event(102, Map("B" -> Array[Tuple]()))),
      (2, Event(102, Map("B" -> Array[Tuple]()))),
      (3, Event(102, Map("B" -> Array[Tuple]()))),

      (0, Event(103, Map("A" -> Array[Tuple](Array(1, 2), Array(2, 3)), "B" -> Array[Tuple]()))),
      (1, Event(103, Map("A" -> Array[Tuple](), "B" -> Array[Tuple](Array(2, 1), Array(2, 3))))),
      (2, Event(103, Map("A" -> Array[Tuple](Array(1, 2)), "B" -> Array[Tuple](Array(2, 1), Array(2, 3))))),
      (3, Event(103, Map("A" -> Array[Tuple](Array(2, 3)), "B" -> Array[Tuple]()))),

      (0, Event(104, Map())),
      (1, Event(104, Map())),
      (2, Event(104, Map())),
      (3, Event(104, Map()))
    )
    // TODO(JS): Check order w.r.t. input events
  }

}
