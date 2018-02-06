package ch.eth.inf.infsec.slicer

import ch.eth.inf.infsec.policy._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable
import scala.util.Random

class HypercubeSlicerTest extends FunSuite with Matchers with PropertyChecks {
  def mkSimpleSlicer(formula: Formula, shares: IndexedSeq[Int], seed: Long = 1234): HypercubeSlicer =
    new HypercubeSlicer(formula,Array.fill(formula.freeVariables.size){(-1, Set.empty: Set[Any])},
      IndexedSeq(shares), seed)

  test("The number of dimensions should equal the number of free variables") {
    mkSimpleSlicer(GenFormula.resolve(Pred("p", ConstInteger(0))), Array[Int]()).dimensions shouldBe 0
    mkSimpleSlicer(GenFormula.resolve(Pred("p", Var("x"))), Array(3)).dimensions shouldBe 1
    mkSimpleSlicer(GenFormula.resolve(Pred("p", Var("x"), Var("y"))), Array(5, 7)).dimensions shouldBe 2
  }

  test("The slicing degree should equal the product of the shares") {
    mkSimpleSlicer(GenFormula.resolve(Pred("p", ConstInteger(0))), Array[Int]()).degree shouldBe 1
    mkSimpleSlicer(GenFormula.resolve(Pred("p", Var("x"))), Array(3)).degree shouldBe 3
    mkSimpleSlicer(GenFormula.resolve(Pred("p", Var("x"), Var("y"))), Array(5, 7)).degree shouldBe 35
  }

  test("Equal values should be mapped to the same slice") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("q", Var("x"), Var("y"))))
    val slicer1 = mkSimpleSlicer(formula, Array(256, 1), 314159)
    forAll { (x: Int, y: Int) =>
      slicer1.slicesOfValuation(Array(Some(x), None)) should contain theSameElementsAs
        slicer1.slicesOfValuation(Array(Some(x), Some(y)))
    }

    val slicer2 = new HypercubeSlicer(formula, Array((0, Set(-1, 0, 2): Set[Any]), (-1, Set.empty: Set[Any])),
      Array(IndexedSeq(256, 1), IndexedSeq(128, 1)), 314159)
    forAll { (x: Int, y: Int) =>
      slicer2.slicesOfValuation(Array(Some(x), None)) should contain theSameElementsAs
        slicer2.slicesOfValuation(Array(Some(x), Some(y)))
    }
  }

  test("Unconstrained variables should be broadcast") {
    val formula = GenFormula.resolve(And(Pred("p", Var("y")), Pred("q", Var("x"), Var("y"))))
    val slicer1 = mkSimpleSlicer(formula, Array(8, 256), 314159)
    forAll { (x: Int, y: Int) =>
      val slices = slicer1.slicesOfValuation(Array(None, Some(y)))
      slices should have size 8
      slices should contain (slicer1.slicesOfValuation(Array(Some(x), Some(y))).head)
    }

    val slicer2 = new HypercubeSlicer(formula, Array((-1, Set.empty: Set[Any]), (0, Set(-1, 0, 2): Set[Any])),
      Array(IndexedSeq(8, 256), IndexedSeq(64, 1)), 314159)
    forAll { (x: Int, y: Int) =>
      val slices = slicer2.slicesOfValuation(Array(None, Some(y)))
      slices should (have size 8 or have size 64)
      slices should contain (slicer2.slicesOfValuation(Array(Some(x), Some(y))).head)
    }
  }

  test("All slices should be used in expectation") {
    def check(slicer: DataSlicer, maxZ: Int = Int.MaxValue): Unit = {
      val random = new Random(314159 + 1)
      var seenSlices = new mutable.HashSet[Int]()
      for (i <- 1 to 1000) {
        val slices = slicer.slicesOfValuation(
          Array(Some(random.nextInt()), Some(random.nextInt()), Some(random.nextInt(maxZ))))
        seenSlices ++= slices
      }

      seenSlices should contain theSameElementsAs Range(0, slicer.degree)
    }

    val formula = GenFormula.resolve(Pred("p", Var("x"), Var("y"), Var("z")))
    check(mkSimpleSlicer(formula, Array(2, 1, 4), 314159))
    check(new HypercubeSlicer(formula,
      Array((-1, Set.empty: Set[Any]), (-1, Set.empty: Set[Any]), (0, (0 until 10).toSet: Set[Any])),
      Array(IndexedSeq(2, 1, 4), IndexedSeq(4, 2, 1)), 314159))
    check(new HypercubeSlicer(formula,
      Array((-1, Set.empty: Set[Any]), (-1, Set.empty: Set[Any]), (0, (0 until 10).toSet: Set[Any])),
      Array(IndexedSeq(2, 1, 4), IndexedSeq(4, 2, 1)), 314159), 1)
    check(new HypercubeSlicer(formula,
      Array((-1, Set.empty: Set[Any]), (-1, Set.empty: Set[Any]), (0, (0 until 10).toSet: Set[Any])),
      Array(IndexedSeq(2, 1, 4), IndexedSeq(4, 2, 1)), 314159), 10)
  }

  test("The result of optimizing a hypercube should match") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("q", Var("x"))))
    val slicer = HypercubeSlicer.optimize(formula, 7, Statistics.constant)

    slicer.formula shouldBe formula
    slicer.degree shouldBe 128
  }

  test("Empty relations are admissible") {
    val formula = GenFormula.resolve(Pred("p", Var("x")))
    val slicer = HypercubeSlicer.optimize(formula, 8, Statistics.simple("p" -> 0.0))
    slicer.degree should be >= 1
  }

  test("Degree one is admissible") {
    val formula = GenFormula.resolve(Pred("p", Var("x")))
    val slicer = HypercubeSlicer.optimize(formula, 0, Statistics.simple("p" -> 100.0))
    slicer.shares should have length 1
    slicer.shares(0) should contain theSameElementsInOrderAs List(1)
  }

  test("Optimal shares with symmetric conditions are symmetric") {
    val formula1 = GenFormula.resolve(
      And(And(Pred("p", Var("x"), Var("y")), Pred("q", Var("y"), Var("z"))), Pred("r", Var("z"), Var("x"))))
    val slicer1 = HypercubeSlicer.optimize(formula1, 9, Statistics.constant)
    slicer1.shares should have length 1
    slicer1.shares(0) should contain theSameElementsInOrderAs List(8, 8, 8)

    val formula2 = GenFormula.resolve(And(Pred("p", Var("x")), Pred("p", Var("y"))))
    val slicer2 = HypercubeSlicer.optimize(formula2, 8, Statistics.constant)
    slicer2.shares should have length 1
    slicer2.shares(0) should contain theSameElementsInOrderAs List(16, 16)
  }

  test("Relation sizes affect optimal shares") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("q", Var("y"))))
    val slicer = HypercubeSlicer.optimize(formula, 10, Statistics.simple("p" -> 100.0, "q" -> 400.0))
    slicer.shares should have length 1
    slicer.shares(0) should contain theSameElementsInOrderAs List(16, 64)
  }

  test("Variables bound to heavy hitters should get one share") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x"), Var("y")), And(Pred("q", Var("x")), Pred("q", Var("y")))))

    val slicer1 = HypercubeSlicer.optimize(formula, 10, new Statistics {
      override def relationSize(relation: String): Double = 100.0

      override def heavyHitters(relation: String, attribute: Int): Set[Any] = (relation, attribute) match {
        case ("p", 0) => Set(0)
        case _ => Set.empty
      }
    })
    slicer1.shares should have length 2
    slicer1.shares(0) should contain theSameElementsInOrderAs List(32, 32)
    slicer1.shares(1) should contain theSameElementsInOrderAs List(1, 1024)

    val slicer2 = HypercubeSlicer.optimize(formula, 10, new Statistics {
      override def relationSize(relation: String): Double = 100.0

      override def heavyHitters(relation: String, attribute: Int): Set[Any] = (relation, attribute) match {
        case ("p", 0) => Set(0)
        case ("p", 1) => Set(1, 2, 3)
        case _ => Set.empty
      }
    })
    slicer2.shares should have length 4
    slicer2.shares(0) should contain theSameElementsInOrderAs List(32, 32)
    slicer2.shares(1) should contain theSameElementsInOrderAs List(1, 1024)
    slicer2.shares(2) should contain theSameElementsInOrderAs List(1024, 1)
    slicer2.shares(3) should contain theSameElementsInOrderAs List(1, 1)
  }
}
