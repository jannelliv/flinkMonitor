package ch.ethz.infsec.slicer

import ch.ethz.infsec.policy._
import ch.ethz.infsec.monitor.{Domain, IntegralValue}
import ch.ethz.infsec.policy.GenFormula
import ch.ethz.infsec.trace.Record
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable
import scala.util.Random

class HypercubeSlicerTest extends FunSuite with Matchers with ScalaCheckPropertyChecks {
  val withHeavy: Gen[Int] = Gen.frequency(
    1 -> -1,
    1 -> 0,
    1 -> 2,
    3 -> Arbitrary.arbInt.arbitrary
  )

  def mkSimpleSlicer(formula: Formula, shares: IndexedSeq[Int], seed: Long = 1234): HypercubeSlicer =
    new HypercubeSlicer(formula,Array.fill(formula.freeVariables.size){(-1, Set.empty: Set[Domain])},
      IndexedSeq(shares), shares.product, seed)

  test("The number of dimensions should equal the number of free variables") {
    mkSimpleSlicer(GenFormula.resolve(Pred("p", Const(0))), Array[Int]()).dimensions shouldBe 0
    mkSimpleSlicer(GenFormula.resolve(Pred("p", Var("x"))), Array(3)).dimensions shouldBe 1
    mkSimpleSlicer(GenFormula.resolve(Pred("p", Var("x"), Var("y"))), Array(5, 7)).dimensions shouldBe 2
  }

  test("The slicing degree should equal the product of the shares") {
    mkSimpleSlicer(GenFormula.resolve(Pred("p", Const(0))), Array[Int]()).degree shouldBe 1
    mkSimpleSlicer(GenFormula.resolve(Pred("p", Var("x"))), Array(3)).degree shouldBe 3
    mkSimpleSlicer(GenFormula.resolve(Pred("p", Var("x"), Var("y"))), Array(5, 7)).degree shouldBe 35
  }

  test("Equal values should be mapped to the same slice") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("q", Var("x"), Var("y"))))
    val slicer1 = mkSimpleSlicer(formula, Array(256, 1), 314159)
    forAll { (x: Int, y: Int) =>
      slicer1.slicesOfValuation(Array(IntegralValue(x), null)) should contain theSameElementsAs
        slicer1.slicesOfValuation(Array(IntegralValue(x), IntegralValue(y)))
    }

    val slicer2 = new HypercubeSlicer(formula, Array((0, Set(-1, 0, 2): Set[Domain]), (-1, Set.empty: Set[Domain])),
      Array(IndexedSeq(256, 1), IndexedSeq(128, 1)), 314159)
    forAll (withHeavy, Arbitrary.arbInt.arbitrary) { (x: Int, y: Int) =>
      slicer2.slicesOfValuation(Array(IntegralValue(x), null)) should contain theSameElementsAs
        slicer2.slicesOfValuation(Array(IntegralValue(x), IntegralValue(y)))
    }
  }

  test("Unconstrained variables should be broadcast") {
    val formula = GenFormula.resolve(And(Pred("p", Var("y")), Pred("q", Var("x"), Var("y"))))
    val slicer1 = mkSimpleSlicer(formula, Array(8, 256), 314159)
    forAll { (x: Int, y: Int) =>
      val slices = slicer1.slicesOfValuation(Array(null, IntegralValue(y)))
      slices should have size 8
      slices should contain (slicer1.slicesOfValuation(Array(IntegralValue(x), IntegralValue(y))).head)
    }

    val slicer2 = new HypercubeSlicer(formula, Array((-1, Set.empty: Set[Domain]), (0, Set(-1, 0, 2): Set[Domain])),
      Array(IndexedSeq(8, 256), IndexedSeq(64, 1)), 314159)
    forAll (Arbitrary.arbInt.arbitrary, withHeavy) { (x: Int, y: Int) =>
      val slices = slicer2.slicesOfValuation(Array(null, IntegralValue(y)))
      slices should (have size 8 or have size 64)
      slices should contain (slicer2.slicesOfValuation(Array(IntegralValue(x), IntegralValue(y))).head)
    }
  }

  test("Records should be sent according to all relevant heavy-hitter configurations") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("q", Var("y"))))
    val slicer = new HypercubeSlicer(formula, Array((0, Set(-1, 0, 2): Set[Domain]), (-1, Set.empty: Set[Domain])),
      Array(IndexedSeq(32, 32), IndexedSeq(1, 2)), 314159)
    forAll (withHeavy, Arbitrary.arbInt.arbitrary) { (x: Int, y: Int) =>
      val pSlices = slicer.slicesOfValuation(Array(IntegralValue(x), null))
      val qSlices = slicer.slicesOfValuation(Array(null, IntegralValue(y)))
      pSlices.intersect(qSlices) should not be empty
    }
  }

  test("All slices should be used in expectation") {
    def check(slicer: DataSlicer, maxZ: Int = Int.MaxValue): Unit = {
      val random = new Random(314159 + 1)
      var seenSlices = new mutable.HashSet[Int]()
      for (_ <- 1 to 1000) {
        val slices = slicer.slicesOfValuation(
          Array(IntegralValue(random.nextInt()), IntegralValue(random.nextInt()), IntegralValue(random.nextInt(maxZ))))
        seenSlices ++= slices
      }

      seenSlices should contain theSameElementsAs Range(0, slicer.degree)
    }

    val formula = GenFormula.resolve(Pred("p", Var("x"), Var("y"), Var("z")))
    val heavyZ = Set[Domain](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    check(mkSimpleSlicer(formula, Array(2, 1, 4), 314159))
    check(new HypercubeSlicer(formula,
      Array((-1, Set.empty: Set[Domain]), (-1, Set.empty: Set[Domain]), (0, heavyZ)),
      Array(IndexedSeq(2, 1, 4), IndexedSeq(4, 2, 1)), 314159))
    check(new HypercubeSlicer(formula,
      Array((-1, Set.empty: Set[Domain]), (-1, Set.empty: Set[Domain]), (0, heavyZ)),
      Array(IndexedSeq(2, 1, 4), IndexedSeq(4, 2, 1)), 314159), 1)
    check(new HypercubeSlicer(formula,
      Array((-1, Set.empty: Set[Domain]), (-1, Set.empty: Set[Domain]), (0, heavyZ)),
      Array(IndexedSeq(2, 1, 4), IndexedSeq(4, 2, 1)), 314159), 10)
    check(new HypercubeSlicer(formula,
      Array((-1, Set.empty: Set[Domain]), (-1, Set.empty: Set[Domain]), (0, heavyZ)),
      Array(IndexedSeq(2, 1, 4), IndexedSeq(3, 3, 1)), 314159), 10)
  }

  test("Unused slices should not receive end markers") {
    val formula = GenFormula.resolve(Pred("p", Var("x"), Var("y"), Var("z")))
    val slicer = new HypercubeSlicer(formula,Array.fill(formula.freeVariables.size){(-1, Set.empty: Set[Domain])},
      IndexedSeq(IndexedSeq(2, 2, 2)), 11, 314159)
    val seenSlices = new mutable.HashSet[Int]()
    slicer.process(Record.markEnd(1234), x => seenSlices += x._1)
    seenSlices should contain theSameElementsAs Range(0, 8)
  }

  test("The result of optimizing a hypercube should match") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("q", Var("x"))))
    val slicer = HypercubeSlicer.optimize(formula, 128, Statistics.constant)

    slicer.formula shouldBe formula
    slicer.degree shouldBe 128
  }

  test("Empty relations are admissible") {
    val formula = GenFormula.resolve(Pred("p", Var("x")))
    val slicer = HypercubeSlicer.optimize(formula, 256, Statistics.simple("p" -> 0.0))
    slicer.degree should be >= 1
  }

  test("Degree one is admissible") {
    val formula = GenFormula.resolve(Pred("p", Var("x")))
    val slicer = HypercubeSlicer.optimize(formula, 1, Statistics.simple("p" -> 100.0))
    slicer.shares should have length 1
    slicer.shares(0) should contain theSameElementsInOrderAs List(1)
  }

  test("Less slices than the given degree may be used") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x"), Var("y")), And(Pred("q", Var("y"), Var("z")),
      Pred("r", Var("z"), Var("x")))))
    val slicer = HypercubeSlicer.optimize(formula, 65,
      Statistics.simple("p" -> 1.0, "q" -> 1.0, "r" -> 1.0))
    slicer.degree shouldBe 64
    slicer.shares should have length 1
    slicer.shares(0) should contain theSameElementsAs List(4, 4, 4)
  }

  test("Optimal shares with symmetric conditions are symmetric") {
    val formula1 = GenFormula.resolve(
      And(And(Pred("p", Var("x"), Var("y")), Pred("q", Var("y"), Var("z"))), Pred("r", Var("z"), Var("x"))))
    val slicer1 = HypercubeSlicer.optimize(formula1, 512, Statistics.constant)
    slicer1.shares should have length 1
    slicer1.shares(0) should contain theSameElementsInOrderAs List(8, 8, 8)

    val formula2 = GenFormula.resolve(And(Pred("p", Var("x")), Pred("p", Var("y"))))
    val slicer2 = HypercubeSlicer.optimize(formula2, 256, Statistics.constant)
    slicer2.shares should have length 1
    slicer2.shares(0) should contain theSameElementsInOrderAs List(16, 16)
  }

  test("Relation sizes affect optimal shares") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("q", Var("y"))))
    val slicer = HypercubeSlicer.optimize(formula, 1024, Statistics.simple("p" -> 100.0, "q" -> 400.0))
    slicer.shares should have length 1
    slicer.shares(0) should contain theSameElementsInOrderAs List(16, 64)
  }

  test("Variables bound to heavy hitters should get one share") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x"), Var("y")), And(Pred("q", Var("x")), Pred("q", Var("y")))))

    val slicer1 = HypercubeSlicer.optimize(formula, 1024, new Statistics {
      override def relationSize(relation: String): Double = 100.0

      override def heavyHitters(relation: String, attribute: Int): Set[Domain] = (relation, attribute) match {
        case ("p", 0) => Set(0)
        case _ => Set.empty
      }
    })
    slicer1.shares should have length 2
    slicer1.shares(0) should contain theSameElementsInOrderAs List(32, 32)
    slicer1.shares(1) should contain theSameElementsInOrderAs List(1, 1024)

    val slicer2 = HypercubeSlicer.optimize(formula, 1024, new Statistics {
      override def relationSize(relation: String): Double = 100.0

      override def heavyHitters(relation: String, attribute: Int): Set[Domain] = (relation, attribute) match {
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

  test("Verdicts should filtered if and only if they do not belong to the slice") {
    val formula = GenFormula.resolve(Pred("p", Var("y"), Var("x")))

    val slicer = HypercubeSlicer.optimize(formula, 1024, new Statistics {
      override def relationSize(relation: String): Double = 100.0

      override def heavyHitters(relation: String, attribute: Int): Set[Domain] = (relation, attribute) match {
        case ("p", 0) => Set(2)
        case _ => Set.empty
      }
    })

    forAll (Arbitrary.arbInt.arbitrary, withHeavy) { (x: Int, y: Int) =>
      val valuation = Array(IntegralValue(x): Domain, IntegralValue(y))
      val verdict = Array(IntegralValue(y): Domain, IntegralValue(x))

      val slices = slicer.slicesOfValuation(valuation)
      slices should have size 1

      slicer.mkVerdictFilter(slices.head)(verdict) shouldBe true
      (0 until 1024).count(i => slicer.mkVerdictFilter(i)(verdict)) shouldBe 1
    }
  }

  // Old optimization routine, limited to powers of 2.
  private def optimizeSingleSetPowerOf2(
                                         formula: Formula,
                                         degreeExp: Int,
                                         statistics: Statistics,
                                         activeVariables: Set[Int]): Array[Int] = {

    require(degreeExp >= 0 && degreeExp < 31)

    var bestCost: Double = Double.PositiveInfinity
    var bestMaxExp: Int = degreeExp
    var bestConfig: List[Int] = Nil

    def atomPartitions(atom: Pred[VariableID], config: List[Int]): Double =
      atom.args.distinct.map {
        case Var(x) if x.isFree => (1 << config(x.freeID)).toDouble
        case _ => 1.0
      }.product

    // This is essentially Algorithm 1 from S. Chu, M. Balazinska and D. Suciu (2015), "From Theory to Practice:
    // Efficient Join Query Evaluation in a Parallel Database System", SIGMOD'15.
    // TODO(JS): Branch-and-bound?
    def search(remainingVars: Int, remainingExp: Int, config: List[Int]): Unit =
      if (remainingVars >= 1) {
        val variable = remainingVars - 1
        val maxExp = if (activeVariables contains variable) remainingExp else 0
        for (e <- 0 to maxExp)
          search(remainingVars - 1, remainingExp - e, e :: config)
      } else {
        // TODO(JS): This cost function does not consider constant constraints nor non-linear atoms.
        val cost = formula.atoms.toSeq.map((atom: Pred[VariableID]) =>
          statistics.relationSize(atom.relation) / atomPartitions(atom, config)).sum
        val maxExp = config.max

        if (cost < bestCost || (cost == bestCost && maxExp < bestMaxExp)) {
          bestConfig = config
          bestCost = cost
          bestMaxExp = maxExp
        }
      }

    search(formula.freeVariables.size, degreeExp, Nil)
    bestConfig.map(e => 1 << e).toArray
  }

  test("Compare optimized shares for power of 2 old implementation") {
    val formula1 = GenFormula.resolve(And(Pred("p", Var("x"), Var("y")), And(Pred("q", Var("y"), Var("z")),
      Pred("r", Var("z"), Var("x")))))

    val statistics1 = Statistics.simple("p" -> 1, "q" -> 1, "r" -> 1)
    val slicer1 = HypercubeSlicer.optimize(formula1, 16, statistics1)
    val reference1 = optimizeSingleSetPowerOf2(formula1, 4, statistics1, Set(0, 1, 2))
    slicer1.shares(0) should contain theSameElementsInOrderAs reference1

    val statistics2 = Statistics.simple("p" -> 1, "q" -> 1, "r" -> 1)
    val slicer2 = HypercubeSlicer.optimize(formula1, 512, statistics2)
    val reference2 = optimizeSingleSetPowerOf2(formula1, 9, statistics2, Set(0, 1, 2))
    slicer2.shares(0) should contain theSameElementsInOrderAs reference2

    val statistics3 = Statistics.simple("p" -> 1000, "q" -> 1, "r" -> 1)
    val slicer3 = HypercubeSlicer.optimize(formula1, 1024, statistics3)
    val reference3 = optimizeSingleSetPowerOf2(formula1, 10, statistics3, Set(0, 1, 2))
    slicer3.shares(0) should contain theSameElementsInOrderAs reference3

    val formula2 = GenFormula.resolve(And(Pred("p", Var("x"), Var("y")), And(Pred("q", Var("y"), Var("z")),
      Pred("r", Var("z"), Var("a")))))

    val statistics4 = Statistics.simple("p" -> 1, "q" -> 1, "r" -> 1)
    val slicer4 = HypercubeSlicer.optimize(formula2, 16, statistics4)
    val reference4 = optimizeSingleSetPowerOf2(formula2, 4, statistics4, Set(0, 1, 2, 3))
    slicer4.shares(0) should contain theSameElementsInOrderAs reference4

    val statistics5 = Statistics.simple("p" -> 1, "q" -> 1, "r" -> 1)
    val slicer5 = HypercubeSlicer.optimize(formula2, 1024, statistics5)
    val reference5 = optimizeSingleSetPowerOf2(formula2, 10, statistics5, Set(0, 1, 2, 3))
    slicer5.shares(0) should contain theSameElementsInOrderAs reference5

    val statistics6 = Statistics.simple("p" -> 1000, "q" -> 1, "r" -> 1)
    val slicer6 = HypercubeSlicer.optimize(formula2, 1024, statistics6)
    val reference6 = optimizeSingleSetPowerOf2(formula2, 10, statistics6, Set(0, 1, 2, 3))
    slicer6.shares(0) should contain theSameElementsInOrderAs reference6
  }

  test("Verdicts must be filtered if two different atoms refer to the same predicate") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("p", Var("y"))))
    val slicer = HypercubeSlicer.optimize(formula, 16, Statistics.simple("p" -> 1))
    slicer.requiresFilter shouldBe true
  }

  test("Verdicts must be filtered if a variable is compared to a constant") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("__eq", Const(IntegralValue(1)), Var("x"))))
    val slicer = HypercubeSlicer.optimize(formula, 16, Statistics.simple("p" -> 1))
    slicer.requiresFilter shouldBe true
  }

  test("Verdicts must be filtered if there are heavy hitters") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("q", Var("y"))))
    val slicer = HypercubeSlicer.optimize(formula, 16, new Statistics {
      override def relationSize(relation: String): Double = 1.0

      override def heavyHitters(relation: String, attribute: Int): Set[Domain] = (relation, attribute) match {
        case ("p", 0) => Set(0)
        case _ => Set.empty
      }
    })
    slicer.requiresFilter shouldBe true
  }

  test("Verdict filtering may be skipped") {
    val formula = GenFormula.resolve(And(Pred("p", Var("x")), Pred("q", Var("y"))))
    val slicer = HypercubeSlicer.optimize(formula, 16, Statistics.simple("p" -> 1, "q" -> 1))
    slicer.requiresFilter shouldBe false
  }
}
