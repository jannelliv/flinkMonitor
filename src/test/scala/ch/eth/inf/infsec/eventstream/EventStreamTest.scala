package ch.eth.inf.infsec
package eventstream

import org.scalatest.{FunSuite,Matchers}
import org.scalatest.OptionValues._


class EventStreamTest extends FunSuite with Matchers {
  test("Stream parse") {
    val invalid = "this is not an event"
    val event = "@1307532861 approve (2,4)(5,6)(2,6) publish (4)(5)"

    parseLine(invalid) shouldBe None
    parseLine(event).value.timestamp shouldBe 1307532861
    parseLine(event).value.structure.size shouldBe 2
    parseLine(event).value.structure("approve").size shouldBe 3
    parseLine(event).value.structure("publish").size shouldBe 2

  }

  test("Print/parse round-trip") {
    val empty1 = Event(1, Map())
    parseLine(printEvent(empty1)).value shouldEqual empty1

    val empty2 = Event(22, Map("p" -> Set[Tuple](Vector()), "q" -> Set[Tuple](Vector())))
    parseLine(printEvent(empty2)).value shouldEqual empty2

    val single = Event(333, Map("p" -> Set[Tuple](Vector(42))))
    parseLine(printEvent(single)).value shouldEqual single

    //val many = Event(444, Map(
    //  "Foo" -> Set[Tuple](Vector(-101, "AbC", 1234), Vector(-102, "dEf", 4321)),
    //  "Bar" -> Set[Tuple](Vector("1", 2))
    //))
    //parseLine(printEvent(many)).value shouldEqual many
  }
}