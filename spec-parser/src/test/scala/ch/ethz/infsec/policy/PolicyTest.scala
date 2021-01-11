package ch.ethz.infsec.policy

import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class PolicyTest extends AnyFunSuite with Matchers {

  val Px = Pred("P", Var("x"))
  val Py = Pred("P", Var("y"))
  val Qxy = Pred("Q", Var("x"), Var("y"))
  val Qii: GenFormula[String] = Pred("Q", Const(7), Const(-42))
  val Qsx = Pred("Q", Const("foo"), Var("x"))
  val Eqxy = Pred("__eq", Var("x"), Var("y"))
  val Eqix = Pred("__eq", Const(-42), Var("x"))
  val Lessxi = Pred("__less", Var("x"), Const(123))

  test("Atomic formulas should be parsed correctly") {
    Policy.parse("TRUE").value shouldBe True()
    Policy.parse("FALSE").value shouldBe False()
    Policy.parse("P()").value shouldBe Pred("P")
    Policy.parse("5_()").value shouldBe Pred("5_")
    Policy.parse("NOTX()").value shouldBe Pred("NOTX")
    Policy.parse("P(x)").value shouldBe Px
    Policy.parse("Q(x,y)").value shouldBe Qxy
    Policy.parse("Q(7, -42)").value shouldBe Qii
    Policy.parse("Q('foo', x)").value shouldBe Qsx
    Policy.parse("Q(\"[foo]\", x)").value shouldBe Qsx
    Policy.parse("((P(x)) )").value shouldBe Px
    Policy.parse("   P(\nx \t\r) ").value shouldBe Px
    Policy.parse("x=y").value shouldBe Eqxy
    Policy.parse("-42 = x").value shouldBe Eqix
    Policy.parse("x < 123").value shouldBe Lessxi
  }

  test("Propositional formulas should be parsed correctly") {
    Policy.parse("NOT P(x)").value shouldBe Not(Px)
    Policy.parse("P(x) OR FALSE").value shouldBe Or(Px, False())
    Policy.parse("P(x) OR P(y) OR FALSE").value shouldBe Or(Or(Px, Py), False())
    Policy.parse("P(x) AND P(y) AND FALSE").value shouldBe And(And(Px, Py), False())
    Policy.parse("(P(x) AND P(y)) OR NOT Q(x, y)").value shouldBe Or(And(Px, Py), Not(Qxy))
    Policy.parse("P(x) AND P(y) OR NOT Q(x, y)").value shouldBe Or(And(Px, Py), Not(Qxy))
    Policy.parse("P(x) AND (P(y) OR NOT Q(x, y))").value shouldBe And(Px, Or(Py, Not(Qxy)))
    Policy.parse("P(x) IMPLIES P(y) IMPLIES Q(x, y)").value shouldBe
      GenFormula.implies(Px, GenFormula.implies(Py, Qxy))
    Policy.parse("P(x) AND P(y) IMPLIES Q(7,-42) OR FALSE").value shouldBe
      GenFormula.implies(And(Px, Py), Or(Qii, False()))
    Policy.parse("P(x) EQUIV P(y) EQUIV Q(x, y)").value shouldBe
      GenFormula.equiv(GenFormula.equiv(Px, Py), Qxy)
    Policy.parse("P(x) EQUIV P(y) IMPLIES FALSE").value shouldBe
      GenFormula.equiv(Px, GenFormula.implies(Py, False()))
    Policy.parse("P(x) AND NOT x = y").value shouldBe And(Px, Not(Eqxy))
    Policy.parse("-42 = x IMPLIES -42 = x").value shouldBe
      GenFormula.implies(Eqix, Eqix)
  }

  test("First-order formulas should be parsed correctly") {
    Policy.parse("EXISTS x. P(x)").value shouldBe Ex("x", Px)
    Policy.parse("FORALL x. P(x)").value shouldBe All("x", Px)
    Policy.parse("EXISTS x, y. P(x)").value shouldBe Ex("x", Ex("y", Px))
    Policy.parse("FORALL x, y. P(x)").value shouldBe All("x", All("y", Px))
    Policy.parse("EXISTS x. EXISTS y. Q(x, y)").value shouldBe Ex("x", Ex("y", Qxy))
    Policy.parse("FORALL x. EXISTS y. Q(x, y)").value shouldBe All("x", Ex("y", Qxy))
    Policy.parse("EXISTS foo. P(x)").value shouldBe Ex("foo", Px)
    Policy.parse("EXISTS x. P(x) IMPLIES P(y)").value shouldBe Ex("x", GenFormula.implies(Px, Py))
    Policy.parse("P(x) AND (FORALL y. P(y))").value shouldBe And(Px, All("y", Py))
  }

  test("Intervals should be parsed correctly") {
    Policy.parse("PREVIOUS FALSE").value shouldBe Prev(Interval.any, False())
    Policy.parse("PREVIOUS [0,0] FALSE").value shouldBe Prev(Interval(0, Some(1)), False())
    Policy.parse("PREVIOUS [3,5) FALSE").value shouldBe Prev(Interval(3, Some(5)), False())
    Policy.parse("PREVIOUS [3,5] FALSE").value shouldBe Prev(Interval(3, Some(6)), False())
    Policy.parse("PREVIOUS (3,5) FALSE").value shouldBe Prev(Interval(4, Some(5)), False())
    Policy.parse("PREVIOUS [3,5) FALSE").value shouldBe Prev(Interval(3, Some(5)), False())
    Policy.parse("PREVIOUS [0,*) FALSE").value shouldBe Prev(Interval(0, None), False())
    Policy.parse("PREVIOUS [3,*] FALSE").value shouldBe Prev(Interval(3, None), False())
    Policy.parse("PREVIOUS [3s,5m) FALSE").value shouldBe Prev(Interval(3, Some(5 * 60)), False())
    Policy.parse("PREVIOUS [3h,5d) FALSE").value shouldBe
      Prev(Interval(3 * 60 * 60, Some(5 * 24 * 60 * 60)), False())
  }

  test("Temporal formulas should be parsed correctly") {
    Policy.parse("PREVIOUS [3,5) P(x)").value shouldBe Prev(Interval(3, Some(5)), Px)
    Policy.parse("NEXT [3,5) P(x)").value shouldBe Next(Interval(3, Some(5)), Px)
    Policy.parse("EVENTUALLY [3,5) P(x)").value shouldBe Eventually(Interval(3, Some(5)), Px)
    Policy.parse("SOMETIMES [3,5) P(x)").value shouldBe Eventually(Interval(3, Some(5)), Px)
    Policy.parse("ONCE [3,5) P(x)").value shouldBe Once(Interval(3, Some(5)), Px)
    Policy.parse("ALWAYS [3,5) P(x)").value shouldBe GenFormula.always(Interval(3, Some(5)), Px)
    Policy.parse("HISTORICALLY [3,5) P(x)").value shouldBe GenFormula.historically(Interval(3, Some(5)), Px)
    Policy.parse("PAST_ALWAYS [3,5) P(x)").value shouldBe GenFormula.historically(Interval(3, Some(5)), Px)
    Policy.parse("P(x) SINCE [3,5) P(y)").value shouldBe Since(Interval(3, Some(5)), Px, Py)
    Policy.parse("P(x) UNTIL [3,5) P(y)").value shouldBe Until(Interval(3, Some(5)), Px, Py)

    Policy.parse("P(x) SINCE [3,5) TRUE SINCE P(y)").value shouldBe
      Since(Interval(3, Some(5)), Px, Since(Interval.any, True(), Py))
    Policy.parse("P(x) AND P(y) UNTIL Q(x, y)").value shouldBe Until(Interval.any, And(Px, Py), Qxy)
    Policy.parse("EXISTS x. P(x) SINCE P(y)").value shouldBe Since(Interval.any, Ex("x", Px), Py)
    Policy.parse("P(x) IMPLIES ONCE Q(x, y)").value shouldBe
      GenFormula.implies(Px, Once(Interval.any, Qxy))
    Policy.parse("P(x) IMPLIES ONCE Q(x,y) AND P(x)").value shouldBe
      GenFormula.implies(Px, Once(Interval.any, And(Qxy, Px)))
  }
}
