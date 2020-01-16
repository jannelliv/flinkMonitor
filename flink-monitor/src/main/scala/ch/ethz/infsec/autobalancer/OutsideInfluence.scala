package ch.ethz.infsec.autobalancer

import java.io._
import java.net.{InetAddress, ServerSocket, Socket}
import java.util.concurrent.LinkedBlockingQueue

import ch.ethz.infsec.monitor.Fact
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._


import scala.util.Random

object OutsideInfluence {
  val statsOutput: OutputTag[(Int, Fact)] = OutputTag[(Int, Fact)]("sampling-output")
}

class OutsideInfluence() extends ProcessFunction[Fact, Fact] {
  var eventsObserved = 0
  var eventObsTillSample = 0
  var sampleEventFreqUpperBoundary = 100 //todo: automatically figured out
  var sampleEventFreqLowerBoundary = 10

  override def processElement(i: Fact, context: ProcessFunction[Fact, Fact]#Context, collector: Collector[Fact]): Unit = {
    if(eventsObserved >= eventObsTillSample && !i.isTerminator && !i.isMeta) {
      eventsObserved = 0
      eventObsTillSample = Random.nextInt(sampleEventFreqUpperBoundary + 1) + sampleEventFreqLowerBoundary
      context.output(OutsideInfluence.statsOutput, (getRuntimeContext.getIndexOfThisSubtask, i))
    } else {
      eventsObserved = eventsObserved + 1
    }
    collector.collect(i)
  }
}
