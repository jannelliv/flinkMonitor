package ch.ethz.infsec.autobalancer

import ch.ethz.infsec.monitor.Fact
import org.scalatest.{FunSuite, Matchers}

// TODO(JS): Update tests.
class WindowStatisticsTest extends FunSuite with Matchers{
  test("endmarkers are not added") {
    val ws = new WindowStatistics(5,1.0,4)
    val rec = Fact.terminator(0)
    rec.isTerminator shouldBe true
    ws.addEvent(rec)
    for(f <- ws.frames) {
      f.relations shouldBe empty
      f.valueOccurances shouldBe empty
    }
    ws.relations shouldBe empty
    //ws.heavyHitter shouldBe empty
  }
  test("adding adds and cycling empties") {
     val ws = new WindowStatistics(5,1.0,4)
    ws.addEvent(Fact.make("a",0))
    ws.frames(ws.lastFrame).relations should not be empty
    ws.nextFrame()
    ws.relations should not be empty
    ws.relations("a") shouldBe 1
    //ws.relationSize("a") shouldBe 1
    ws.nextFrame()
    ws.nextFrame()
    ws.nextFrame()
    ws.nextFrame()
    ws.relations.size shouldBe 1
    ws.relations("a") shouldBe 1
    //ws.relationSize("a") shouldBe 1

    ws.nextFrame()
    ws.relations shouldBe empty
    //ws.relationSize("a") shouldBe 0
  }

  /*
  test("moving forward by timestamp works") {
    val ws = new WindowStatistics(5, 1.0,4)
    val ori = ws.lastFrame
    ws.addEvent(Fact.make("a",0))
    ws.lastFrame shouldBe ori
    ws.addEvent(Fact.make("b",1))
    ws.lastFrame shouldBe ((ori+1) % 5)
    ws.addEvent(Fact.make("b",5))
    ws.lastFrame shouldBe ((ori+5) % 5)
    ws.addEvent(Fact.make("a",34))
    ws.lastFrame shouldBe ((ori+34) % 5)

    val ws2 = new WindowStatistics(5, 3.4,4)
    val ori2 = ws2.lastFrame
    ws2.addEvent(Fact.make("a",0))
    ws2.lastFrame shouldBe ori2
    ws2.addEvent(Fact.make("a",34))
    ws2.lastFrame shouldBe ((ori2+(34/3.4)) % 5)
    ws2.addEvent(Fact.make("a",47))
    ws2.lastFrame shouldBe Math.floor((ori2+(47/3.4)) % 5)
  }
  */

  /*
  test("heavy hitters") {
    //todo: since our definition of heavy hitter may change this test is brittle
    val ws = new WindowStatistics(5, 10.0,2)
    ws.addEvent(Fact.make("a",0,util.Collections.singletonList(2)))
    ws.addEvent(Fact.make("a",2,util.Collections.singletonList(3)))
    ws.addEvent(Fact.make("a",4,util.Collections.singletonList(4)))
    ws.addEvent(Fact.make("a",6,util.Collections.singletonList(5)))
    ws.addEvent(Fact.make("a",8,util.Collections.singletonList(6)))
    ws.addEvent(Fact.make("a",10,util.Collections.singletonList(2)))
    ws.nextFrame()
    ws.heavyHitters("a",0).size shouldBe 1
    //TODO: fix
    //ws.heavyHitters("a",0)(2) shouldBe true
    ws.heavyHitter.size shouldBe 1
  }
  */
}
