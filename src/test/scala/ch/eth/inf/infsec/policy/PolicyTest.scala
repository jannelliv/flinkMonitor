package ch.eth.inf.infsec.policy

import org.scalatest.{FunSuite, Matchers}
import org.scalatest.EitherValues._

class PolicyTest extends FunSuite with Matchers {

  val Px = Pred("P", Free(-1, "x"))
  val Py = Pred("P", Free(-1, "y"))
  val Qxy = Pred("Q", Free(-1, "x"), Free(-1, "y"))
  val Qll = Pred("Q", Const(7l), Const(-42l))
  val Qsx = Pred("Q", Const("foo"), Free(-1, "x"))

  test("Atomic formulas should be parsed correctly") {
    Policy.parse("TRUE").right.value shouldBe Formula.True
    Policy.parse("FALSE").right.value shouldBe False()
    Policy.parse("P()").right.value shouldBe Pred("P")
    Policy.parse("P(x)").right.value shouldBe Px
    Policy.parse("Q(x,y)").right.value shouldBe Qxy
    Policy.parse("Q(7, -42)").right.value shouldBe Qll
    Policy.parse("Q('foo', x)").right.value shouldBe Qsx
    Policy.parse("Q(\"[foo]\", x)").right.value shouldBe Qsx
    Policy.parse("((P(x)) )").right.value shouldBe Px
    Policy.parse("   P(\nx \t\r) ").right.value shouldBe Px
  }

  test("Propositional formulas should be parsed correctly") {
    Policy.parse("NOT P(x)").right.value shouldBe Not(Px)
    Policy.parse("P(x) OR FALSE").right.value shouldBe Or(Px, False())
    Policy.parse("P(x) OR P(y) OR FALSE").right.value shouldBe Or(Or(Px, Py), False())
    Policy.parse("P(x) AND P(y) AND FALSE").right.value shouldBe And(And(Px, Py), False())
    Policy.parse("(P(x) AND P(y)) OR NOT Q(x, y)").right.value shouldBe Or(And(Px, Py), Not(Qxy))
    Policy.parse("P(x) AND P(y) OR NOT Q(x, y)").right.value shouldBe Or(And(Px, Py), Not(Qxy))
    Policy.parse("P(x) AND (P(y) OR NOT Q(x, y))").right.value shouldBe And(Px, Or(Py, Not(Qxy)))
    Policy.parse("P(x) IMPLIES P(y) IMPLIES Q(x, y)").right.value shouldBe
      Formula.Implies(Px, Formula.Implies(Py, Qxy))
    Policy.parse("P(x) AND P(y) IMPLIES Q(7,-42) OR FALSE").right.value shouldBe
      Formula.Implies(And(Px, Py), Or(Qll, False()))
    Policy.parse("P(x) EQUIV P(y) EQUIV Q(x, y)").right.value shouldBe
      Formula.Equiv(Formula.Equiv(Px, Py), Qxy)
    Policy.parse("P(x) EQUIV P(y) IMPLIES FALSE").right.value shouldBe
      Formula.Equiv(Px, Formula.Implies(Py, False()))
  }

  test("First-order formulas should be parsed correctly") {
    Policy.parse("EXISTS x. P(x)").right.value shouldBe Ex("x", Px)
    Policy.parse("FORALL x. P(x)").right.value shouldBe All("x", Px)
    Policy.parse("EXISTS x, y. P(x)").right.value shouldBe Ex("x", Ex("y", Px))
    Policy.parse("FORALL x, y. P(x)").right.value shouldBe All("x", All("y", Px))
    Policy.parse("EXISTS x. EXISTS y. Q(x, y)").right.value shouldBe Ex("x", Ex("y", Qxy))
    Policy.parse("FORALL x. EXISTS y. Q(x, y)").right.value shouldBe All("x", Ex("y", Qxy))
    Policy.parse("EXISTS foo. P(x)").right.value shouldBe Ex("foo", Px)
    Policy.parse("EXISTS x. P(x) IMPLIES P(y)").right.value shouldBe Ex("x", Formula.Implies(Px, Py))
    Policy.parse("P(x) AND (FORALL y. P(y))").right.value shouldBe And(Px, All("y", Py))
  }

  test("Intervals should be parsed correctly") {
    Policy.parse("PREVIOUS FALSE").right.value shouldBe Prev(Interval.any, False())
    Policy.parse("PREVIOUS [0,0] FALSE").right.value shouldBe Prev(Interval(0, Some(1)), False())
    Policy.parse("PREVIOUS [3,5) FALSE").right.value shouldBe Prev(Interval(3, Some(5)), False())
    Policy.parse("PREVIOUS [3,5] FALSE").right.value shouldBe Prev(Interval(3, Some(6)), False())
    Policy.parse("PREVIOUS (3,5) FALSE").right.value shouldBe Prev(Interval(4, Some(5)), False())
    Policy.parse("PREVIOUS [3,5) FALSE").right.value shouldBe Prev(Interval(3, Some(5)), False())
    Policy.parse("PREVIOUS [0,*) FALSE").right.value shouldBe Prev(Interval(0, None), False())
    Policy.parse("PREVIOUS [3,*] FALSE").right.value shouldBe Prev(Interval(3, None), False())
    Policy.parse("PREVIOUS [3s,5m) FALSE").right.value shouldBe Prev(Interval(3, Some(5 * 60)), False())
    Policy.parse("PREVIOUS [3h,5d) FALSE").right.value shouldBe
      Prev(Interval(3 * 60 * 60, Some(5 * 24 * 60 * 60)), False())
  }

  test("Temporal formulas should be parsed correctly") {
    Policy.parse("PREVIOUS [3,5) P(x)").right.value shouldBe Prev(Interval(3, Some(5)), Px)
    Policy.parse("NEXT [3,5) P(x)").right.value shouldBe Next(Interval(3, Some(5)), Px)
    Policy.parse("EVENTUALLY [3,5) P(x)").right.value shouldBe Formula.Eventually(Interval(3, Some(5)), Px)
    Policy.parse("SOMETIMES [3,5) P(x)").right.value shouldBe Formula.Eventually(Interval(3, Some(5)), Px)
    Policy.parse("ONCE [3,5) P(x)").right.value shouldBe Formula.Once(Interval(3, Some(5)), Px)
    Policy.parse("ALWAYS [3,5) P(x)").right.value shouldBe Formula.Always(Interval(3, Some(5)), Px)
    Policy.parse("HISTORICALLY [3,5) P(x)").right.value shouldBe Formula.Historically(Interval(3, Some(5)), Px)
    Policy.parse("PAST_ALWAYS [3,5) P(x)").right.value shouldBe Formula.Historically(Interval(3, Some(5)), Px)
    Policy.parse("P(x) SINCE [3,5) P(y)").right.value shouldBe Since(Interval(3, Some(5)), Px, Py)
    Policy.parse("P(x) UNTIL [3,5) P(y)").right.value shouldBe Until(Interval(3, Some(5)), Px, Py)

    Policy.parse("P(x) SINCE [3,5) TRUE SINCE P(y)").right.value shouldBe
      Since(Interval(3, Some(5)), Px, Since(Interval.any, Formula.True, Py))
    Policy.parse("P(x) AND P(y) UNTIL Q(x, y)").right.value shouldBe Until(Interval.any, And(Px, Py), Qxy)
    Policy.parse("EXISTS x. P(x) SINCE P(y)").right.value shouldBe Since(Interval.any, Ex("x", Px), Py)
  }

}
