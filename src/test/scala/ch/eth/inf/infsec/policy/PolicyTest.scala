package ch.eth.inf.infsec.policy

import org.scalatest.{FunSuite, Matchers}
import org.scalatest.EitherValues._

class PolicyTest extends FunSuite with Matchers {

  test("Plain formulas should be parsed correctly") {
    Policy.parse("TRUE").right.value shouldBe Not(False())
    Policy.parse("P(x) OR FALSE").right.value shouldBe Or(Pred("P", Free(-1, "x")), False())
    Policy.parse("P(x) OR Q(x) OR FALSE").right.value shouldBe
      Or(Or(Pred("P", Free(-1, "x")), Pred("Q", Free(-1, "x"))), False())
    Policy.parse("(P(x) AND P(y)) OR NOT Q(x, y)").right.value shouldBe
      Or(And(Pred("P", Free(-1, "x")), Pred("P", Free(-1, "y"))), Not(Pred("Q", Free(-1, "x"), Free(-1, "y"))))
  }

}
