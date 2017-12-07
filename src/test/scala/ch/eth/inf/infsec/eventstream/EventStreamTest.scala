package ch.eth.inf.infsec
package eventstream

import org.scalatest.{FunSuite,Matchers}
import org.scalatest.EitherValues._


class EventStreamTest extends FunSuite with Matchers {
  test("Stream parse") {
    val invlaid = "this is not an event"
    val event = "@1307532861 approve (2,4)(5,6)(2,6) publish (4)(5)"

    parseLine(invlaid) shouldBe None
    parseLine(event) should not be None
    parseLine(event).get.timestamp shouldBe 1307532861
    parseLine(event).get.structure.size shouldBe 2
    parseLine(event).get.structure("approve").size shouldBe 3
    parseLine(event).get.structure("publish").size shouldBe 2

  }
}