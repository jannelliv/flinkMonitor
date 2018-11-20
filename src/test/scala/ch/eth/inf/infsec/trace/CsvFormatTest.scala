package ch.eth.inf.infsec.trace

import org.scalatest.{FunSuite, Inside, Matchers}

class CsvFormatTest extends FunSuite with Matchers {
  test("Parsing individual lines") {
    CsvParser.parseLine("rel1, tp = 123, ts = 456, a = 10") shouldBe (123, 456, "rel1", Tuple(10))
    // TODO(JS): Rewrite these
    Inside.inside(CsvParser.parseLine("rel1, tp = 0, ts = 0, a = -1")) { case (0, 0, "rel1", tuple) =>
      tuple shouldBe Tuple(-1)
    }
    Inside.inside(CsvParser.parseLine("rel1, tp = 0, ts = 0, a = 10x")) { case (0, 0, "rel1", tuple) =>
      tuple shouldBe Tuple("10x")
    }
    Inside.inside(CsvParser.parseLine("rel1, tp = 123, ts = 456, a = 12, c = 10, b = 11")) {
      case (123, 456, "rel1", tuple) => tuple shouldBe Tuple(12, 10, 11)
    }
    Inside.inside(CsvParser.parseLine("rel1,tp =0,ts=0,  a=-1\n")) { case (0, 0, "rel1", tuple) =>
      tuple shouldBe Tuple(-1)
    }
    Inside.inside(CsvParser.parseLine("rel1 , tp=0,ts=  +1\t\n")) { case (0, 1, "rel1", tuple) =>
      tuple shouldBe Tuple()
    }
    Inside.inside(CsvParser.parseLine("rel1 , tp=0,ts=  12 , a=\n")) { case (0, 12, "rel1", tuple) =>
      tuple shouldBe Tuple("")
    }
    Inside.inside(CsvParser.parseLine("rel1 , tp=0,ts=  12 , a= , b =abc\n")) { case (0, 12, "rel1", tuple) =>
      tuple shouldBe Tuple("", "abc")
    }
  }

  test("Parsing events with multiple tuples") {
    val input1 = "withdraw, tp = 0, ts = 1, u = u183, a = 42"
    val input2 = input1 ::
      "withdraw, tp = 0, ts = 1, u = u440, a = 1" ::
      "login, tp = 0, ts = 1, u = u321" ::
      "withdraw, tp = 0, ts = 1, u = u105, a = 21" :: Nil

    CsvFormat.createParser().processAll(List(input1)) should contain theSameElementsInOrderAs List(
      EventRecord(1, "withdraw", Tuple("u183", 42)),
      Record.markEnd(1)
    )

    CsvFormat.createParser().processAll(input2) should contain theSameElementsInOrderAs List(
      EventRecord(1, "withdraw", Tuple("u183", 42)),
      EventRecord(1, "withdraw", Tuple("u440", 1)),
      EventRecord(1, "login", Tuple("u321")),
      EventRecord(1, "withdraw", Tuple("u105", 21)),
      Record.markEnd(1)
    )
  }

  test("Parsing multiple events") {
    val input = "withdraw, tp = 0, ts = 1, u = u109, a = 32\n" +
      "withdraw, tp = 0, ts = 1, u = u438, a = 55\n" +
      "withdraw, tp = 1, ts = 1, u = u642, a = 58\n" +
      "login, tp = 2, ts = 3, u = u321\n" +
      "withdraw, tp = 2, ts = 3, u = u285, a = 60\n" +
      "withdraw, tp = 2, ts = 3, u = u383, a = 67\n"

    CsvFormat.createParser().processAll(input.split("\n")) should contain theSameElementsInOrderAs List(
      EventRecord(1, "withdraw", Tuple("u109", 32)),
      EventRecord(1, "withdraw", Tuple("u438", 55)),
      Record.markEnd(1),
      EventRecord(1, "withdraw", Tuple("u642", 58)),
      Record.markEnd(1),
      EventRecord(3, "login", Tuple("u321")),
      EventRecord(3, "withdraw", Tuple("u285", 60)),
      EventRecord(3, "withdraw", Tuple("u383", 67)),
      Record.markEnd(3)
    )
  }
}
