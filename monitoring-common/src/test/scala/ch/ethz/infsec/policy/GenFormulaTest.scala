package ch.ethz.infsec.policy

import org.scalatest.{FunSuite, Matchers, Inside}
import org.scalatest.enablers.Emptiness

class GenFormulaTest extends FunSuite with Matchers {
  // NOTE(JS): No idea why this is not provided automatically ...
  implicit def emptinessOfSetPrev[V]: Emptiness[Set[Pred[V]]] =
    new Emptiness[Set[Pred[V]]] {
      def isEmpty(set: Set[Pred[V]]): Boolean = set.isEmpty
    }

  test("Set of atoms") {
    val px = Pred("p", Var("x"))
    val qy = Pred("q", Var("y"))

    True().atoms shouldBe empty
    False().atoms shouldBe empty
    px.atoms should contain only px
    Not(px).atoms should contain only px
    Or(False(), qy).atoms should contain only qy
    Or(px, px).atoms should contain only px
    Or(px, qy).atoms should contain only (px, qy)
    And(px, qy).atoms should contain only (px, qy)
    Ex("b", px).atoms should contain only px
    Ex("x", px).atoms should contain only px
    All("b", px).atoms should contain only px
    All("x", px).atoms should contain only px
    Prev(Interval(0, Some(1)), px).atoms should contain only px
    Next(Interval(0, Some(1)), px).atoms should contain only px
    Since(Interval(0, Some(1)), px, qy).atoms should contain only (px, qy)
    Trigger(Interval(0, Some(1)), px, qy).atoms should contain only (px, qy)
    Until(Interval(0, Some(1)), px, qy).atoms should contain only (px, qy)
    Release(Interval(0, Some(1)), px, qy).atoms should contain only (px, qy)

    And(px, Ex("b", Prev(Interval(0, Some(0)), Or(qy, Pred("r", Var("z"), Var("x"))))))
      .atoms should contain only (px, qy, Pred("r", Var("z"), Var("x")))
  }

  test("Set of free variables") {
    Const(42).freeVariables shouldBe empty
    Const("foo").freeVariables shouldBe empty
    Var("x").freeVariables should contain only "x"

    True().freeVariables shouldBe empty
    False().freeVariables shouldBe empty
    Pred("p").freeVariables shouldBe empty
    Pred("p", Var("x"), Var("x"), Const(2), Var("y")).freeVariables should contain only ("x", "y")

    val x = Var("x")
    val y = Var("y")
    val px = Pred("p", x)
    val qy = Pred("q", y)

    Not(px).freeVariables should contain only "x"
    Or(False(), qy).freeVariables should contain only "y"
    Or(px, px).freeVariables should contain only "x"
    Or(px, qy).freeVariables should contain only ("x", "y")
    And(px, qy).freeVariables should contain only ("x", "y")
    Ex("b", px).freeVariables should contain only "x"
    Ex("x", px).freeVariables shouldBe empty
    All("b", px).freeVariables should contain only "x"
    All("x", px).freeVariables shouldBe empty
    Prev(Interval(0, Some(1)), px).freeVariables should contain only "x"
    Next(Interval(0, Some(1)), px).freeVariables should contain only "x"
    Since(Interval(0, Some(1)), px, qy).freeVariables should contain only ("x", "y")
    Trigger(Interval(0, Some(1)), px, qy).freeVariables should contain only ("x", "y")
    Until(Interval(0, Some(1)), px, qy).freeVariables should contain only ("x", "y")
    Release(Interval(0, Some(1)), px, qy).freeVariables should contain only ("x", "y")

    And(px, Ex("y", Prev(Interval(0, Some(0)), Or(qy, Pred("r", Var("z"), x)))))
      .freeVariables should contain only ("x", "z")
  }

  test("Resolve detects free and bound variables") {
    val pyxy = Pred("p", Var("y"), Var("x"), Var("y"))

    val phi1 = GenFormula.resolve(pyxy)
    Inside.inside (phi1) { case Pred("p", Var(v1), Var(v2), Var(v3)) =>
      v1.nameHint shouldBe "y"
      v1 shouldBe 'free
      v1.freeID shouldBe 1

      v2.nameHint shouldBe "x"
      v2 shouldBe 'free
      v2.freeID shouldBe 0

      v3.nameHint shouldBe "y"
      v3 shouldBe 'free
      v3.freeID shouldBe 1

      v1 should be theSameInstanceAs v3
    }

    val phi2 = GenFormula.resolve(All("x", pyxy))
    Inside.inside (phi2) { case All(vq, Pred("p", Var(v1), Var(v2), Var(v3))) =>
      vq.nameHint shouldBe "x"
      vq should not be 'free
      vq.freeID should be < 0

      v1.nameHint shouldBe "y"
      v1 shouldBe 'free
      v1.freeID shouldBe 0

      v1 should be theSameInstanceAs v3
      vq should be theSameInstanceAs v2
    }

    val phi3 = GenFormula.resolve(Ex("y", pyxy))
    Inside.inside (phi3) { case Ex(vq, Pred("p", Var(v1), Var(v2), Var(v3))) =>
      vq.nameHint shouldBe "y"
      vq should not be 'free
      vq.freeID should be < 0

      v2.nameHint shouldBe "x"
      v2 shouldBe 'free
      v2.freeID shouldBe 0

      vq should be theSameInstanceAs v1
      v1 should be theSameInstanceAs v3
    }
  }

  test("Printing after resolve") {
    def roundTrip(phi: GenFormula[String]): GenFormula[String] = GenFormula.print(GenFormula.resolve(phi))

    val x = Var("x")
    val y = Var("y")
    val px = Pred("p", x)
    val py = Pred("p", y)

    roundTrip(True()) shouldBe True()
    roundTrip(False()) shouldBe False()
    roundTrip(Not(px)) shouldBe Not(px)
    roundTrip(Or(False(), px)) shouldBe Or(False(), px)
    roundTrip(Or(px, px)) shouldBe Or(px, px)
    roundTrip(Or(px, py)) shouldBe Or(px, py)
    roundTrip(And(px, py)) shouldBe And(px, py)
    roundTrip(Ex("u", px)) shouldBe Ex("u", px)
    roundTrip(Ex("x", px)) shouldBe Ex("x", px)
    roundTrip(All("u", px)) shouldBe All("u", px)
    roundTrip(All("x", px)) shouldBe All("x", px)
    roundTrip(Prev(Interval.any, px)) shouldBe Prev(Interval.any, px)
    roundTrip(Next(Interval.any, px)) shouldBe Next(Interval.any, px)
    roundTrip(Since(Interval.any, px, py)) shouldBe Since(Interval.any, px, py)
    roundTrip(Trigger(Interval.any, px, py)) shouldBe Trigger(Interval.any, px, py)
    roundTrip(Until(Interval.any, px, py)) shouldBe Until(Interval.any, px, py)
    roundTrip(Release(Interval.any, px, py)) shouldBe Release(Interval.any, px, py)

    roundTrip(All("x", Ex("x", px))) shouldBe All("x", Ex("x_1", Pred("p", Var("x_1"))))
    roundTrip(All("x", Ex("y", px))) shouldBe All("x", Ex("y", px))
    roundTrip(All("x", Ex("y", py))) shouldBe All("x", Ex("y", py))
    roundTrip(All("x", And(Ex("x", px), px))) shouldBe All("x", And(Ex("x_1", Pred("p", Var("x_1"))), px))
    roundTrip(Ex("x", Ex("x", All("x", px)))) shouldBe Ex("x", Ex("x_1", All("x_2", Pred("p", Var("x_2")))))

    roundTrip(And(px, Ex("x", px))) shouldBe And(px, Ex("x_1", Pred("p", Var("x_1"))))
    roundTrip(Or(All("y", And(py, px)), py)) shouldBe Or(All("y_1", And(Pred("p", Var("y_1")), px)), py)
  }

  test("Checking valid intervals") {
    Interval.any.check shouldBe empty
    Interval(1, None).check shouldBe empty
    Interval(321, None).check shouldBe empty
    Interval(0, Some(1)).check shouldBe empty
    Interval(1, Some(100)).check shouldBe empty
    Interval(321, Some(322)).check shouldBe empty
  }

  test("Checking invalid intervals") {
    Interval(-1, None).check should not be empty
    Interval(-1, Some(1)).check should not be empty
    Interval(0, Some(0)).check should not be empty
    Interval(1, Some(0)).check should not be empty
    Interval(321, Some(123)).check should not be empty
    Interval(-321, Some(-322)).check should not be empty
  }

  test("Checking valid formulas") {
    val x = Var("x")
    val y = Var("y")
    val px = Pred("p", x)
    val py = Pred("p", y)

    True().check shouldBe empty
    False().check shouldBe empty
    Not(px).check shouldBe empty
    Or(False(), px).check shouldBe empty
    Or(px, px).check shouldBe empty
    Or(px, py).check shouldBe empty
    And(px, py).check shouldBe empty
    Ex("u", px).check shouldBe empty
    Ex("x", px).check shouldBe empty
    All("u", px).check shouldBe empty
    All("x", px).check shouldBe empty
    Prev(Interval.any, px).check shouldBe empty
    Next(Interval.any, px).check shouldBe empty
    Since(Interval.any, px, py).check shouldBe empty
    Trigger(Interval.any, px, py).check shouldBe empty
    Until(Interval.any, px, py).check shouldBe empty
    Release(Interval.any, px, py).check shouldBe empty

    Prev(Interval(1, None), px).check shouldBe empty
    Prev(Interval(0, Some(1)), px).check shouldBe empty
    Prev(Interval(1, Some(100)), px).check shouldBe empty

    And(Pred("P", Var("x")), Ex("b", Prev(Interval.any, Or(Pred("Q"), Pred("r", Var("z"), Var("x"))))))
      .check shouldBe empty
    /*
    All("x", Ex("x", px)).check shouldBe empty
    All("x", Ex("y", px)).check shouldBe empty
    All("x", Ex("y", py)).check shouldBe empty

    And(Pred("p", x), Ex("x", Pred("p", x))).check shouldBe
      And(Pred("p", x0), Ex("x", Pred("p", Bound(0, "x"))))
    Or(All("y", And(py, px)), Pred("p", y)).check shouldBe
      Or(All("y", And(Pred("p", Bound(0, "y")), Pred("p", x0))), Pred("p", y1))
      */
  }

  test("Checking invalid formulas") {
    val bad1 = Interval(-1, Some(1))
    val bad2 = Interval(5, Some(4))

    Prev(bad1, True()).check should not be empty
    Prev(bad2, True()).check should not be empty
    Next(bad1, True()).check should not be empty
    Next(bad2, True()).check should not be empty
    Since(bad1, True(), True()).check should not be empty
    Trigger(bad1, True(), True()).check should not be empty
    Until(bad1, True(), True()).check should not be empty
    Release(bad1, True(), True()).check should not be empty

    val prevBad: GenFormula[String] = Prev(bad1, True())
    val prevGood: GenFormula[String] = Prev(Interval.any, True())

    Or(prevBad, prevGood).check should not be empty
    Or(prevGood, prevBad).check should not be empty
    Or(prevBad, prevBad).check should not be empty
    And(prevBad, prevGood).check should not be empty
    And(prevGood, prevBad).check should not be empty
    And(prevBad, prevBad).check should not be empty
    All("x", prevBad).check should not be empty
    Ex("x", prevBad).check should not be empty
    Prev(Interval.any, prevBad).check should not be empty
    Next(Interval.any, prevBad).check should not be empty
    Since(Interval.any, prevBad, True()).check should not be empty
    Trigger(Interval.any, prevBad, True()).check should not be empty
    Until(Interval.any, True(), prevBad).check should not be empty
    Release(Interval.any, True(), prevBad).check should not be empty

    And(Pred("P", Var("x")), Ex("b", Prev(bad2, Or(Pred("Q"), Pred("r", Var("z"), Var("x"))))))
      .check should not be empty
  }
}
