package ch.ethz.infsec.trace

import org.scalatest.{FunSuite, Matchers}

class DejavuFormatTest extends FunSuite with Matchers {
  import DejavuFormat.createParser

  test("Parsing an invalid line") {
    val invalid = "this is not an event"
    createParser().processAll(List(invalid)) shouldBe empty
  }

  test("Parsing a single event") {
    val event = " approve , f2, 4"

    createParser().processAll(List(event)) should contain theSameElementsAs List(
      EventRecord(0, "approve", Tuple("f2", 4)),
      Record.markEnd(0)
    )
  }

  test("Parsing multiple events") {
    val events = List(" P ,4", "inv alid!", "Q,7 ")
    createParser().processAll(events) should contain theSameElementsAs List(
      EventRecord(0, "P", Tuple(4)),
      Record.markEnd(0),
      EventRecord(0, "Q", Tuple(7)),
      Record.markEnd(0)
    )
  }

  test("Print/parse round-trip") {
    def roundTrip(records: Seq[Record]): Seq[Record] =
      createParser().processAll((new DejavuPrinter[Unit]).processAll(records.map(Left(_))).map(_.left.get.in))

    def nullTS(r: Record) = Record(0, r.label, r.data, r.command, r.parameters)



    val empty1 = List(Record.markEnd(1))
    roundTrip(empty1) should contain theSameElementsAs empty1.map(r => nullTS(r))

    val empty2 = List(
      EventRecord(22, "p", Tuple()),
      EventRecord(22, "q", Tuple()),
      Record.markEnd(22)
    )
    an [IllegalStateException] should be thrownBy roundTrip(empty2)

    val single = List(
      EventRecord(333, "p", Tuple(42)),
      Record.markEnd(333)
    )
    roundTrip(single) should contain theSameElementsAs single.map(r => nullTS(r))

    val many = List(
      EventRecord(444, "Foo", Tuple(-101, "AbC", 1234)),
      Record.markEnd(444),
      EventRecord(444, "Foo", Tuple(-102, "dEf", 4321)),
      Record.markEnd(444),
      EventRecord(444, "Bar", Tuple("hello, world!")),
      Record.markEnd(444)
    )
    roundTrip(many) should contain theSameElementsAs many.map(r => nullTS(r))
  }


}
