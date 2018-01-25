package ch.eth.inf.infsec.slicer

import ch.eth.inf.infsec.policy._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable
import scala.util.Random

class HypercubeSlicerTest extends FunSuite with Matchers with PropertyChecks {
  test("The slicing degree should equal the product of the shares") {
    new HypercubeSlicer(GenFormula.resolve(Pred("p", ConstInteger(0))), Array[Int]()).degree shouldBe 1
    new HypercubeSlicer(GenFormula.resolve(Pred("p", Var("x"))), Array(3)).degree shouldBe 3
    new HypercubeSlicer(GenFormula.resolve(Pred("p", Var("x"), Var("y"))), Array(5, 7)).degree shouldBe 35
  }

  test("Equal values should be mapped to the same slice") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("q", Var("x"), Var("y"))))
    val slicer = new HypercubeSlicer(formula, Array(256, 1), 314159)
    forAll { (x: Int, y: Int) =>
      slicer.slicesOfValuation(Array(Some(x), None)) should contain theSameElementsAs
        slicer.slicesOfValuation(Array(Some(x), Some(y)))
    }
  }

  test("Unconstrained variables should be broadcast") {
    val formula = GenFormula.resolve(Pred("p", Var("x")))
    val slicer = new HypercubeSlicer(formula, Array(8, 256), 314159)
    forAll { (x: Int, y: Int) =>
      val slices = slicer.slicesOfValuation(Array(None, Some(y)))
      slices should have size 8
      slices should contain (slicer.slicesOfValuation(Array(Some(x), Some(y))).head)
    }
  }

  test("All slices should be used in expectation") {
    val formula = GenFormula.resolve(Pred("p", Var("x")))
    val slicer = new HypercubeSlicer(formula, Array(2, 1, 4), 314159)

    val random = new Random(314159 + 1)
    var seenSlices = new mutable.HashSet[Int]()
    for (i <- 1 to 1000) {
      val slices = slicer.slicesOfValuation(
        Array(Some(random.nextInt()), Some(random.nextInt()), Some(random.nextInt())))
      seenSlices ++= slices
    }

    seenSlices should contain theSameElementsAs Range(0, slicer.degree)
  }

  test("The result of optimizing a hypercube should match") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("q", Var("x"))))
    val statistics = new Statistics {
      override def relationSize(relation: String): Double = 100.0
    }
    val slicer = HypercubeSlicer.optimize(formula, 7, statistics)

    slicer.formula shouldBe formula
    slicer.degree shouldBe 128
  }

  test("Empty relations are admissible") {
    val statistics = new Statistics {
      override def relationSize(relation: String): Double = 0.0
    }
    val slicer = HypercubeSlicer.optimize(GenFormula.resolve(Pred("p", Var("x"))), 8, statistics)
    slicer.degree should be >= 1
  }

  test("Degree one is admissible") {
    val statistics = new Statistics {
      override def relationSize(relation: String): Double = 100.0
    }
    val slicer = HypercubeSlicer.optimize(GenFormula.resolve(Pred("p", Var("x"))), 0, statistics)
    slicer.shares should contain theSameElementsInOrderAs List(1)
  }

  test("Optimal shares with symmetric conditions are symmetric") {
    val statistics = new Statistics {
      override def relationSize(relation: String): Double = 100.0
    }

    val formula1 = GenFormula.resolve(
      And(And(Pred("p", Var("x"), Var("y")), Pred("q", Var("y"), Var("z"))), Pred("r", Var("z"), Var("x"))))
    val slicer1 = HypercubeSlicer.optimize(formula1, 9, statistics)
    slicer1.shares should contain theSameElementsInOrderAs List(8, 8, 8)

    val formula2 = GenFormula.resolve(And(Pred("p", Var("x")), Pred("p", Var("y"))))
    val slicer2 = HypercubeSlicer.optimize(formula2, 8, statistics)
    slicer2.shares should contain theSameElementsInOrderAs List(16, 16)
  }

  test("Relation sizes affect optimal shares") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("q", Var("y"))))
    val statistics = new Statistics {
      override def relationSize(relation: String): Double = relation match {
        case "p" => 100.0
        case "q" => 400.0
      }
    }
    val slicer = HypercubeSlicer.optimize(formula, 10, statistics)
    slicer.shares should contain theSameElementsInOrderAs List(16, 64)
  }

}
