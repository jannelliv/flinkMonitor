package ch.eth.inf.infsec.policy

import org.scalatest.{FunSuite, Matchers}
import org.scalatest.EitherValues._

class FormulaTest extends FunSuite with Matchers {
  test("Set of atoms") {
    val px = Pred("p", Free(0, "x"))
    val py = Pred("q", Free(1, "y"))

    False().atoms shouldBe empty
    px.atoms should contain only px
    Not(px).atoms should contain only px
    Or(False(), py).atoms should contain only py
    Or(px, py).atoms should contain only (px, py)
    And(px, py).atoms should contain only (px, py)
    Ex("b", px).atoms should contain only px
    All("b", px).atoms should contain only px
    Prev(Interval(0, 1), px).atoms should contain only px
    Next(Interval(0, 1), px).atoms should contain only px
    Once(Interval(0, 1), px).atoms should contain only px
    Eventually(Interval(0, 1), px).atoms should contain only px
    Historically(Interval(0, 1), px).atoms should contain only px
    Always(Interval(0, 1), px).atoms should contain only px
    Since(Interval(0, 1), px, py).atoms should contain only (px, py)
    Until(Interval(0, 1), px, py).atoms should contain only (px, py)

    And(px, Ex("b", Prev(Interval(0, 0), Or(py, Pred("r", Free(2, "z"), Free(0, "x"))))))
      .atoms should contain only (px, py, Pred("r", Free(2, "z"), Free(0, "x")))
  }

  test("Set of free variables") {
    Const(42).freeVariables shouldBe empty
    Const("foo").freeVariables shouldBe empty
    Bound(0, "x").freeVariables shouldBe empty
    Free(0, "x").freeVariables should contain only Free(0, "x")

    False().freeVariables shouldBe empty
    Pred("p").freeVariables shouldBe empty
    Pred("p", Free(0, "x"), Free(1, "x"), Const(2), Free(3, "y")).freeVariables should
      contain only (Free(0, "x"), Free(1, "x"), Free(3, "y"))

    val x = Free(0, "x")
    val y = Free(1, "y")
    val px = Pred("p", x)
    val py = Pred("q", y)

    Not(px).freeVariables should contain only x
    Or(False(), py).freeVariables should contain only y
    Or(px, Pred("q", Bound(1, "y"))).freeVariables should contain only x
    Or(px, py).freeVariables should contain only (x, y)
    And(px, py).freeVariables should contain only (x, y)
    Ex("b", px).freeVariables should contain only x
    All("b", px).freeVariables should contain only x
    Prev(Interval(0, 1), px).freeVariables should contain only x
    Next(Interval(0, 1), px).freeVariables should contain only x
    Once(Interval(0, 1), px).freeVariables should contain only x
    Eventually(Interval(0, 1), px).freeVariables should contain only x
    Historically(Interval(0, 1), px).freeVariables should contain only x
    Always(Interval(0, 1), px).freeVariables should contain only x
    Since(Interval(0, 1), px, py).freeVariables should contain only (x, y)
    Until(Interval(0, 1), px, py).freeVariables should contain only (x, y)

    And(px, Ex("b", Prev(Interval(0, 0), Or(py, Pred("r", Free(2, "z"), x)))))
      .freeVariables should contain only (x, y, Free(2, "z"))
  }

  private def check(formula: Formula): Either[String, Formula] =
    formula.check(Context.empty())

  test("Checking valid formulas") {
    val x = Free(-1, "x")
    val x0 = Free(0, "x")
    val y = Free(-1, "y")
    val y1 = Free(1, "y")

    check(False()).right.value shouldBe False()
    check(Not(Pred("p", x))).right.value shouldBe Not(Pred("p", x0))
    check(Or(False(), Pred("p", x))).right.value shouldBe Or(False(), Pred("p", x0))

    // TODO(JS): More tests

    check(And(Pred("p", x), Ex("x", Pred("p", x)))).right.value shouldBe
      And(Pred("p", x0), Ex("x", Pred("p", Bound(0, "x"))))
  }

}
