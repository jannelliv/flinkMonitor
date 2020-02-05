package ch.ethz.infsec.autobalancer

import java.io._
import java.util.Base64

import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.policy.Formula
import ch.ethz.infsec.slicer.HypercubeSlicer
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object OutsideInfluence {
  val statsOutput: OutputTag[(Int, Fact)] = OutputTag[(Int, Fact)]("sampling-output")
}

class OutsideInfluence(degree : Int, formula : Formula, windowSize : Double) extends ProcessFunction[Fact, Fact] {
  var eventsObserved = 0
  var eventObsTillSample = 0
  var sampleEventFreqUpperBoundary = 100 //todo: automatically figured out
  var sampleEventFreqLowerBoundary = 10
  var oldSlicer: HypercubeSlicer = HypercubeSlicer.optimize(formula, degree, ConstantHistogram())
  var newSlicer: HypercubeSlicer = HypercubeSlicer.optimize(formula, degree, ConstantHistogram())
  var slicerTracker: HypercubeCostSlicerTracker = _
  var windowStatistics: WindowStatistics = new WindowStatistics(1, windowSize, degree)
  var shouldSample: Boolean = false
  var isInit: Boolean = false
  var started: Boolean = false
  var startTime: Long = 0

  def startSampling(): Unit = {
    shouldSample = true
    startTime = System.currentTimeMillis()
  }

  override def processElement(i: Fact, context: ProcessFunction[Fact, Fact]#Context, collector: Collector[Fact]): Unit = {
    if(!started) {
      context.output(OutsideInfluence.statsOutput,(getRuntimeContext.getIndexOfThisSubtask, Fact.meta("hello_decider", "")))
      started = true
    }
    if (i.isMeta) {
      //println(s"outside influence got meta fact: $i")
      if (i.getName == "init_slicer_tracker") {
        val initial_slicer = i.getArgument(0).asInstanceOf[java.lang.String]
        oldSlicer.unstringify(initial_slicer)
        newSlicer.unstringify(initial_slicer)
        isInit = true
        slicerTracker = new HypercubeCostSlicerTracker(oldSlicer, degree)
        startSampling()
        return
      }
      if (i.getName == "start_sampling") {
        if (!isInit)
          throw new RuntimeException("INVARIANT: start_sample before init_slicer_tracker")

        val newStrategy = i.getArgument(0).asInstanceOf[java.lang.String]
        oldSlicer.unstringify(newSlicer.stringify)
        newSlicer.unstringify(newStrategy)
        slicerTracker.reset(newSlicer, oldSlicer)
        startSampling()
        return
      }
    }
    if (shouldSample) {
      if (System.currentTimeMillis() > startTime + 1500) {
        val costFact = Fact.meta("slicing_cost", slicerTracker.toBase64)
        context.output(OutsideInfluence.statsOutput, (getRuntimeContext.getIndexOfThisSubtask, costFact))
        val histogram = NormalHistogram(windowStatistics.getHistogram, degree)
        val histFact = Fact.meta("histogram", histogram.toBase64)
        context.output(OutsideInfluence.statsOutput, (getRuntimeContext.getIndexOfThisSubtask, histFact))
        eventsObserved = 0
        eventObsTillSample = 0
        shouldSample = false
        windowStatistics.nextFrame()
      } else {
        if(eventsObserved >= eventObsTillSample && !i.isTerminator && !i.isMeta) {
          eventsObserved = 0
          eventObsTillSample = Random.nextInt(sampleEventFreqUpperBoundary + 1) + sampleEventFreqLowerBoundary
          slicerTracker.addEvent(i)
          windowStatistics.addEvent(i)
        } else {
          eventsObserved = eventsObserved + 1
        }
      }
    }
    collector.collect(i)
  }
}

object HypercubeCostSlicerTracker {
  type SlicerTrackerCost = ArrayBuffer[Int]

  def biCosts(c1: SlicerTrackerCost, c2: SlicerTrackerCost): SlicerTrackerCost =
    c1.zip(c2).map(k => k._1 + k._2)

  def fromBase64(s: String): HypercubeCostSlicerTracker = {
    val data = Base64.getDecoder.decode(s)
    val ois = new ObjectInputStream(new ByteArrayInputStream(data))
    val o = ois.readObject
    ois.close()
    o.asInstanceOf[HypercubeCostSlicerTracker]
  }
}

class HypercubeCostSlicerTracker(initialSlicer : HypercubeSlicer, degree : Int) extends Serializable {
  type Record = Fact
  var first : HypercubeSlicer = initialSlicer
  var second : HypercubeSlicer = initialSlicer
  require(degree > 0)

  var firstArr : ArrayBuffer[Int] = ArrayBuffer.fill(degree)(0)
  var secondArr : ArrayBuffer[Int] = ArrayBuffer.fill(degree)(0)

  private def cost(arrayBuffer: ArrayBuffer[Int]) : Int = {
    var max = 0
    for(v <- arrayBuffer) {
      if(v > max)
        max = v
    }
    max
  }

  def bothCosts(): (Int, Int) = (cost(firstArr), cost(secondArr))

  def merge(other: HypercubeCostSlicerTracker) : Unit = {
    firstArr = HypercubeCostSlicerTracker.biCosts(other.firstArr, firstArr)
    secondArr = HypercubeCostSlicerTracker.biCosts(other.secondArr, secondArr)
  }

  def reset(_first : HypercubeSlicer, _second : HypercubeSlicer): Unit = {
    if (_first.degree != degree)
      throw new RuntimeException("INVARIANT: _first.degree == degree")
    if (_first.degree != _second.degree)
      throw new RuntimeException("INVARIANT: _first.degree == _second.degree")
    first = _first
    second = _second
    firstArr = ArrayBuffer.fill(degree)(0)
    secondArr = ArrayBuffer.fill(degree)(0)
  }

  def addEventInternal(event:Record,slicer:HypercubeSlicer,arr:ArrayBuffer[Int]): Unit = {
    val res = slicer.process(event)
    res.foreach(x => { arr(x._1) += 1 })
  }

  def addEvent(event:Record): Unit = {
    addEventInternal(event,first,firstArr)
    addEventInternal(event,second,secondArr)
  }

  def toBase64: String = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(this)
    oos.close()
    Base64.getEncoder.encodeToString(baos.toByteArray)
  }
}
