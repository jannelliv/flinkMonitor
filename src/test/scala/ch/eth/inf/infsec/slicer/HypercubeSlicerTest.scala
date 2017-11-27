package ch.eth.inf.infsec.slicer

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSuite, Matchers}

class HypercubeSlicerTest extends FunSuite with Matchers with PropertyChecks {
  test("The slicing degree should equal the product of the shares") {
    new HypercubeSlicer(Formula(Set()), Array[Int]()).degree shouldBe 1
    new HypercubeSlicer(Formula(Set(Atom("p", FreeVar(0)))), Array(3))
      .degree shouldBe 3
    new HypercubeSlicer(Formula(Set(Atom("p", FreeVar(0), FreeVar(1)))), Array(5, 7))
      .degree shouldBe 35
  }

  test("Equal values should be mapped to the same slice") {
    val formula = Formula(Set(Atom("p", FreeVar(0)), Atom("q", FreeVar(0), FreeVar(1))))
    val slicer = new HypercubeSlicer(formula, Array(256, 1), 314159)
    forAll { (x: Int, y: Int) =>
      slicer.slicesOfValuation(Array(Some(x), None)) should contain theSameElementsAs
        slicer.slicesOfValuation(Array(Some(x), Some(y)))
    }
  }

  test("Unconstrained variables should be broadcast") {
    val formula = Formula(Set(Atom("p", FreeVar(0))))
    val slicer = new HypercubeSlicer(formula, Array(8, 256), 314159)
    forAll { (x: Int, y: Int) =>
      val slices = slicer.slicesOfValuation(Array(None, Some(y)))
      slices should have size 8
      slices should contain (slicer.slicesOfValuation(Array(Some(x), Some(y))).head)
    }
  }

  // TODO(JS): More tests ...
}
