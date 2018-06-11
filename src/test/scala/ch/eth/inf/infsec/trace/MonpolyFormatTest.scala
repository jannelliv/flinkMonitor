package ch.eth.inf.infsec.trace

import org.scalatest.{FunSuite, Matchers}

class MonpolyFormatTest extends FunSuite with Matchers {
  import MonpolyFormat.createParser

  test("Parsing an invalid line") {
    val invalid = "this is not an event"
    createParser().processAll(List(invalid)) shouldBe empty
  }

  test("Parsing a single event") {
    val event = "@1307532861 approve (2,4)(5,6) (2, 6 ) publish (4)(5)"
    val timestamp = 1307532861L

    createParser().processAll(List(event)) should contain theSameElementsAs List(
      Record(timestamp, "approve", Tuple(2, 4)),
      Record(timestamp, "approve", Tuple(5, 6)),
      Record(timestamp, "approve", Tuple(2, 6)),
      Record(timestamp, "publish", Tuple(4)),
      Record(timestamp, "publish", Tuple(5)),
      Record.markEnd(timestamp)
    )
  }

  test("Parsing multiple events") {
    val events = List("@123 P(4)", "invalid!", "@456 Q(7)")
    createParser().processAll(events) should contain theSameElementsAs List(
      Record(123, "P", Tuple(4)),
      Record.markEnd(123),
      Record(456, "Q", Tuple(7)),
      Record.markEnd(456)
    )
  }

  test("Print/parse round-trip") {
    def roundTrip(records: Seq[Record]): Seq[Record] =
      createParser().processAll((new MonpolyPrinter).processAll(records))

    val empty1 = List(Record.markEnd(1))
    roundTrip(empty1) should contain theSameElementsAs empty1

    val empty2 = List(
      Record(22, "p", Tuple()),
      Record(22, "q", Tuple()),
      Record.markEnd(22)
    )
    roundTrip(empty2) should contain theSameElementsAs empty2

    val single = List(
      Record(333, "p", Tuple(42)),
      Record.markEnd(333)
    )
    roundTrip(single) should contain theSameElementsAs single

    //val many = Event(444, Map(
    //  "Foo" -> Set[Tuple](Vector(-101, "AbC", 1234), Vector(-102, "dEf", 4321)),
    //  "Bar" -> Set[Tuple](Vector("1", 2))
    //))
    //parseLine(printEvent(many)).value shouldEqual many
  }

  test("Filtering verdicts") {
    val filter = new MonpolyVerdictFilter(_ => t => t(0) match {
      case IntegralValue(i) => i > 0
      case _ => false
    })

    filter.processAll(List(
      "@0. (time point 0): true",
      "@12. (time point 34): (101)(0)(102)",
      "This is not a verdict.",
      "@34. (time point 567): (101,102) (104, 105) (-1,103)"
    )) should contain theSameElementsInOrderAs List(
      "@0. (time point 0): true",
      "@12. (time point 34): (101) (102)",
      "@34. (time point 567): (101,102) (104,105)"
    )
  }
}