package ch.ethz.infsec.autobalancer

import ch.ethz.infsec.monitor.IntegralValue
import ch.ethz.infsec.trace
import ch.ethz.infsec.trace.Record
import org.scalatest.{FunSuite, Matchers}

class WindowStatisticsTest extends FunSuite with Matchers{
  test("endmarkers are not added") {
    val ws = new WindowStatistics(5,1.0,4)
    val rec = Record(0,"",trace.Tuple(),"","")
    rec.isEndMarker shouldBe true
    ws.addEvent(rec)
    for(f <- ws.frames) {
      f.relations shouldBe empty
      f.valueOccurances shouldBe empty
    }
    ws.relations shouldBe empty
    ws.heavyHitter shouldBe empty
  }
  test("adding adds and cycling empties") {
     val ws = new WindowStatistics(5,1.0,4)
    ws.addEvent(Record(0,"a",trace.Tuple(),"",""))
    ws.frames(ws.lastFrame).relations should not be empty
    ws.nextFrame()
    ws.relations should not be empty
    ws.relations("a") shouldBe 1
    ws.relationSize("a") shouldBe 1
    ws.nextFrame()
    ws.nextFrame()
    ws.nextFrame()
    ws.nextFrame()
    ws.relations.size shouldBe 1
    ws.relations("a") shouldBe 1
    ws.relationSize("a") shouldBe 1

    ws.nextFrame()
    ws.relations shouldBe empty
    ws.relationSize("a") shouldBe 0
  }
  test("moving forward by timestamp works") {
    val ws = new WindowStatistics(5, 1.0,4)
    val ori = ws.lastFrame
    ws.addEvent(Record(0,"a",trace.Tuple(),"",""))
    ws.lastFrame shouldBe ori
    ws.addEvent(Record(1,"b",trace.Tuple(),"",""))
    ws.lastFrame shouldBe ((ori+1) % 5)
    ws.addEvent(Record(5,"b",trace.Tuple(),"",""))
    ws.lastFrame shouldBe ((ori+5) % 5)
    ws.addEvent(Record(34,"a",trace.Tuple(),"",""))
    ws.lastFrame shouldBe ((ori+34) % 5)

    val ws2 = new WindowStatistics(5, 3.4,4)
    val ori2 = ws2.lastFrame
    ws2.addEvent(Record(0,"a",trace.Tuple(),"",""))
    ws2.lastFrame shouldBe ori2
    ws2.addEvent(Record(34,"a",trace.Tuple(),"",""))
    ws2.lastFrame shouldBe ((ori2+(34/3.4)) % 5)
    ws2.addEvent(Record(47,"a",trace.Tuple(),"",""))
    ws2.lastFrame shouldBe Math.floor((ori2+(47/3.4)) % 5)
  }

  test("heavy hitters") {
    //todo: since our definition of heavy hitter may change this test is brittle
    val ws = new WindowStatistics(5, 10.0,2)
    ws.addEvent(Record(0,"a",trace.Tuple(IntegralValue(2)),"",""))
    ws.addEvent(Record(2,"a",trace.Tuple(IntegralValue(3)),"",""))
    ws.addEvent(Record(4,"a",trace.Tuple(IntegralValue(4)),"",""))
    ws.addEvent(Record(6,"a",trace.Tuple(IntegralValue(5)),"",""))
    ws.addEvent(Record(8,"a",trace.Tuple(IntegralValue(6)),"",""))
    ws.addEvent(Record(10,"a",trace.Tuple(IntegralValue(2)),"",""))
    ws.nextFrame()
    ws.heavyHitters("a",0).size shouldBe 1
    ws.heavyHitters("a",0)(2) shouldBe true
    ws.heavyHitter.size shouldBe 1
  }
}
