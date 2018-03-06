package ch.eth.inf.infsec.trace

import org.scalatest.{FunSuite, Inside, Matchers}

class CsvFormatTest extends FunSuite with Matchers {
  test("Parsing individual lines") {
    Inside.inside(CsvParser.parseLine("rel1, tp = 123, ts = 456, a = 10")) { case (123, 456, "rel1", tuple) =>
      tuple should contain only 10
    }
    Inside.inside(CsvParser.parseLine("rel1, tp = 0, ts = 0, a = -1")) { case (0, 0, "rel1", tuple) =>
      tuple should contain only -1
    }
    Inside.inside(CsvParser.parseLine("rel1, tp = 0, ts = 0, a = 10x")) { case (0, 0, "rel1", tuple) =>
      tuple should contain only "10x"
    }
    Inside.inside(CsvParser.parseLine("rel1, tp = 123, ts = 456, a = 12, c = 10, b = 11")) {
      case (123, 456, "rel1", tuple) => tuple should contain inOrderOnly(12, 10, 11)
    }
    Inside.inside(CsvParser.parseLine("rel1,tp =0,ts=0,  a=-1\n")) { case (0, 0, "rel1", tuple) =>
      tuple should contain only -1
    }
    Inside.inside(CsvParser.parseLine("rel1 , tp=0,ts=  +1\t\n")) { case (0, 1, "rel1", tuple) =>
      tuple shouldBe empty
    }
    Inside.inside(CsvParser.parseLine("rel1 , tp=0,ts=  12 , a=\n")) { case (0, 12, "rel1", tuple) =>
      tuple should contain only ""
    }
    Inside.inside(CsvParser.parseLine("rel1 , tp=0,ts=  12 , a= , b =abc\n")) { case (0, 12, "rel1", tuple) =>
      tuple should contain inOrderOnly ("", "abc")
    }
  }

  test("Parsing events with multiple tuples") {
    val input1 = "withdraw, tp = 0, ts = 1, u = u183, a = 42"
    val input2 = input1 ::
      "withdraw, tp = 0, ts = 1, u = u440, a = 1" ::
      "login, tp = 0, ts = 1, u = u321" ::
      "withdraw, tp = 0, ts = 1, u = u105, a = 21" :: Nil

    CsvFormat.createParser().parseLines(List(input1)) should contain only
      Event(1, Map("withdraw" -> Seq(IndexedSeq("u183", 42))))
    CsvFormat.createParser().parseLines(input2) should contain only
      Event(1, Map("login" -> Seq(IndexedSeq("u321")), "withdraw" -> Seq(
        IndexedSeq("u183", 42), IndexedSeq("u440", 1), IndexedSeq("u105", 21)
      )))
  }

  test("Parsing multiple events") {
    val input = "withdraw, tp = 0, ts = 1, u = u109, a = 32\n" +
      "withdraw, tp = 0, ts = 1, u = u438, a = 55\n" +
      "withdraw, tp = 1, ts = 1, u = u642, a = 58\n" +
      "login, tp = 2, ts = 3, u = u321\n" +
      "withdraw, tp = 2, ts = 3, u = u285, a = 60\n" +
      "withdraw, tp = 2, ts = 3, u = u383, a = 67\n"

    CsvFormat.createParser().parseLines(input.split("\n")) should contain inOrderOnly (
      Event(1, Map("withdraw" -> Seq(IndexedSeq("u109", 32), IndexedSeq("u438", 55)))),
      Event(1, Map("withdraw" -> Seq(IndexedSeq("u642", 58)))),
      Event(3, Map("login" -> Seq(IndexedSeq("u321")),
        "withdraw" -> Seq(IndexedSeq("u285", 60), IndexedSeq("u383", 67))))
    )
  }
}
