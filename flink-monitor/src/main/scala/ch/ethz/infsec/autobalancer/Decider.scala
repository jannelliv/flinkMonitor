package ch.ethz.infsec.autobalancer

import java.io.FileWriter
import java.util

import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.policy.Formula
import ch.ethz.infsec.slicer.{HypercubeSlicer, Statistics}
import ch.ethz.infsec.tools.Rescaler.RescaleInitiator
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class DummyDecider extends FlatMapFunction[(Int, Fact), Fact] {
  override def flatMap(t: (Int, Fact), collector: Collector[Fact]): Unit = {

  }
}

//problem: how to solve "final bit of stream needs to be processed to"
//proposed solution: "end of stream message"
//TODO: properly parameterize
class AllState(_rfmf : DeciderFlatMapSimple)
  extends FlatMapFunction[Fact, Fact] with ListCheckpointed[DeciderFlatMapSimple] with Serializable
{
  var rfmf : DeciderFlatMapSimple = _rfmf
  type State = DeciderFlatMapSimple

  var started = true
  var logFile : FileWriter = new FileWriter("AllState.log", true)
  logFile.write("started\n")

  private def getState: DeciderFlatMapSimple = {
    logFile.write("getState\n")
    logFile.flush()
    rfmf.shutdown()
    rfmf
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[DeciderFlatMapSimple] =
    util.Collections.singletonList(getState)

  override def restoreState(state: util.List[DeciderFlatMapSimple]): Unit = {
    assert(state.size()==1)
    rfmf = state.get(0)
    rfmf.startup()
  }

  override def flatMap(in: Fact, c: Collector[Fact]): Unit = {
    rfmf.flatMap(in, c)
  }

  def terminate(f: Fact => Unit): Unit = {
    logFile.write("terminate\n")
    logFile.flush()
  }
}

trait CostSlicerTracker[SlicingStrategy] {
  def costFirst : Int
  def costSecond : Int
  def reset(_first : SlicingStrategy, _second : SlicingStrategy): Unit
  def addEvent(event:Fact): Unit
}

class HypercubeCostSlicerTracker(initialSlicer : HypercubeSlicer, degree : Int) extends CostSlicerTracker[HypercubeSlicer] with Serializable {
  type Record = Fact
  var first : HypercubeSlicer = initialSlicer
  var second : HypercubeSlicer = initialSlicer

  var firstArr : ArrayBuffer[Int] = ArrayBuffer.fill(degree)(0)
  var secondArr : ArrayBuffer[Int] = ArrayBuffer.fill(degree)(0)

  def cost(arrayBuffer: ArrayBuffer[Int]) : Int = {
    var max = 0
    for(v <- arrayBuffer) {
      if(v > max)
        max = v
    }
    max
  }

  def costFirst : Int = cost(firstArr)

  def costSecond : Int = cost(secondArr)

  def reset(_first : HypercubeSlicer, _second : HypercubeSlicer): Unit = {
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
}


class DeciderFlatMapSimple(degree : Int, formula : Formula, windowSize : Double) extends DeciderFlatMap[HypercubeSlicer](degree,windowSize,false,true) {
  override def firstSlicing: HypercubeSlicer = {
    HypercubeSlicer.optimize(
      formula, degree,Statistics.constant)
  }
  override def getSlicingStrategy(ws : WindowStatistics) : HypercubeSlicer = {
    HypercubeSlicer.optimize(
      formula, degree, ws)
  }

  //the slicing cost refers to the ongoing cost paid over some time interval t
  //further, we do not care about the total performance cost but about the -balancing-
  //so the cost of slicing is the load placed on the node with the most load over some time interval t

  //we can further assume that the cost of monitoring for a node is relative to the amount of events it receives
  //(although in practice it is likely that certain events incur higher performance costs than others due to the details of a MFOTL formula, we ignore that for now)
  //thus we can define the cost of slicing as: max_{s \in Slices} f(events(s))
  //we for now define f as a linear function, but in theory it could be a more complicated function

  //the problem are then:
  //1. for a data set, obtain the slicing
  //2. from the slicing, get the total event counts
  //3. define a reasonable f
  override def slicingCost(strat: HypercubeSlicer, events: ArrayBuffer[Fact]): Double = {
    //todo: emulate on the record, account for the one with the highest output
    val arr = strat.processAll(events)
    val retArr = ArrayBuffer.fill(degree)(0)
    for(v <- arr) {
      retArr(v._1) += 1
    }
    var max = 0
    for(v <- retArr) {
      if(v > max)
        max = v
    }
    max * avgMaxProcessingTime
  }

  override var costSlicerTracker: CostSlicerTracker[HypercubeSlicer] = new HypercubeCostSlicerTracker(firstSlicing, degree)

  //the cost of adaptation consists essentially of two parts
  //1. the cost of disturbing the current operation for a rebalance
  //2. the cost of shuffling all the data around for the actual rebalance

  //estimating either one is difficult, 1 entirely depends on the performance of the hypercube slicer for making a new slicing and the framework for dealing with a change of flow
  //ideally though 1 is negligible
  //the cost of shuffling all the data around is also hard to estimate, as it deals with networks as well as the details of the monitoring algo
  //we can assume that the cost of adaptation is at most linear in the total amount of memory used by all nodes (or rather O(n*m) at worst, with n being amount of nodes and m being amount of memory, which would be all nodes transmitting all their memory to all other nodes)
  override def adaptationCost(strat: HypercubeSlicer, ws: WindowStatistics): Double = {
    return shutdownTime
  }
  override def sendAdaptMessage(c: Collector[Fact], strat: HypercubeSlicer) = {
    val param:String = strat.stringify
    c.collect(Fact.meta("set_slicer", param))
    new RescaleInitiator().rescale(degree)
  }
}


abstract class DeciderFlatMap[SlicingStrategy]
                (degree : Int, windowSize : Double,
                 isBuffering : Boolean, shouldDumpData : Boolean)
  extends FlatMapFunction[Fact, Fact] with Serializable {

  def firstSlicing : SlicingStrategy
  def getSlicingStrategy(ws : WindowStatistics) : SlicingStrategy
  def slicingCost(strat:SlicingStrategy,events:ArrayBuffer[Fact]) : Double
  def adaptationCost(strat:SlicingStrategy,ws:WindowStatistics) : Double
  def sendAdaptMessage(c: Collector[Fact], strat: SlicingStrategy) : Unit

  var avgMaxProcessingTime : Double = 0.01

  val windowStatistics = new WindowStatistics(1,windowSize, degree)
  var lastSlicing = firstSlicing
  var sliceCandidate = lastSlicing
  var eventBuffer = ArrayBuffer[Fact]()

  var costSlicerTracker : CostSlicerTracker[SlicingStrategy]

  var shutdownTime : Double = 1000.0

  var assumedFutureStableWindowsAmount : Double = 1.0

  //sampling section
  val isSampling = false //todo: param
  var eventsObserved = 0
  var eventObsTillSample = 0
  var sampleEventFreqUpperBoundary = 100 //todo: automatically figured out
  var sampleEventFreqLowerBoundary = 10 //todo: automatically figured out

  var shouldAdapt = false
  var triggeredAdapt = false
  var doAdaptStuff = false
  var adaptComplete = false

  private var lastShutdownInitiatedTime : Long = 0
  //TODO: reconsider if we might have to put this into monpoly.process for correctness
  //TODO: consider putting this in flatMap instead because we know that at that point the topology is all working. Downside: if we don't get events immediately after topology is up fully, our timing will be wrong.
  def handleReopenTime() : Unit = {
    val lastShutdownCompletedTime = System.currentTimeMillis()
    if(lastShutdownInitiatedTime == 0) {
      //TODO: better startup time based on how long first startup took
      shutdownTime = 1000
    }else{
      shutdownTime = lastShutdownCompletedTime - lastShutdownInitiatedTime
    }
  }


  def makeAdaptDecision(c : Collector[Fact]) : Unit = {
    val prevSlicerForLog = lastSlicing
    //while in the middle of an adapting we don't re-adapt
    if(shouldAdapt || triggeredAdapt || doAdaptStuff || adaptComplete)
      return
    doAdaptStuff = true
    if(costSlicerTracker.costFirst * avgMaxProcessingTime + adaptationCost(sliceCandidate,windowStatistics) / assumedFutureStableWindowsAmount < costSlicerTracker.costSecond * avgMaxProcessingTime) {
      lastSlicing = sliceCandidate
      shouldAdapt = true
//      triggeredAdapt = true
//      c.collect(generateMessage(lastSlicing))
    }else{
//      c.collect(Fact.meta("gapt",""))
    }
    if(shouldDumpData) {
      val realtimeTimestamp = System.currentTimeMillis()
      logFile.write("--------- Status at time: "+realtimeTimestamp+" ---------\n")
      logFile.write("adapting: "+shouldAdapt+"\n")
      logFile.write("current slicer: "+prevSlicerForLog+"\n")
      if(shouldAdapt) {
        logFile.write("new slicer: "+sliceCandidate.toString+"\n")
      }else{
        logFile.write("rejected slicer candidate: "+sliceCandidate+"\n")
      }
      logFile.write("cost for current: "+costSlicerTracker.costSecond+" units\n")
      logFile.write("cost for new: "+costSlicerTracker.costFirst+" units\n")
      logFile.write("average max processing time: "+avgMaxProcessingTime+" ms/unit\n")
      logFile.write("predicted stability: "+assumedFutureStableWindowsAmount+" windows\n")
      logFile.write("shutdown memory: "+shutdownMemory+" kB\n")
      logFile.write("shutdown time: "+shutdownTime+" ms\n")
      logFile.write("----- End Status at time: "+realtimeTimestamp+" -----\n")
      logFile.flush()
    }
  }

  var shutdownMemory : Long = 0

  @transient  var logFile : FileWriter = _
  def logCommand(com:String,params:String): Unit = {
    if(shouldDumpData) {
          logFile.write("==== RECEIVED COMMAND: "+com+" "+params+" ====\n")
      logFile.flush()
    }
  }

  @transient  var started = false
  //  var tempF : FileWriter = _
  //  var tempF2 : FileWriter = _
  override def flatMap(event: Fact, c: Collector[Fact]): Unit = {
    if(!started) {
      started = true
      logFile = new FileWriter("decider.log",true)
      logFile.write("started\n")
      logFile.flush()
      /*     tempF = new FileWriter("events.log",false)
           tempF2 = new FileWriter("eventsOut.log",false)
     */    }
    /*    if(!event.isEndMarker) {
          tempF.write(event.toMonpoly + "\n")
          tempF.flush()
        }
    */
    if(adaptComplete) {
      shouldAdapt = false
      triggeredAdapt = false
      doAdaptStuff = false
      adaptComplete = false

//    c.collect(Fact.meta("gsdt",""))
      c.collect(Fact.meta("gsdms",""))
    }
    if (event.isMeta) {
        //CommandRecord(com,params)
        val com = event.getName
        val params = List(event.getArguments).mkString
        if(com == "gaptr") {
          avgMaxProcessingTime = params.toDouble / 1000.0
          if(avgMaxProcessingTime < 0.0001)
            avgMaxProcessingTime = 0.0001
          logCommand(com,params)
/*        } else if(com == "gsdtr") {
          //function approximation code
          shutdownTime = params.toDouble
          logCommand(com,params)*/
        } else if(com == "gsdmsr") {
          shutdownMemory = params.toLong
          logCommand(com,params)
        }else{
          c.collect(event)
        }
      } else {
        if (!event.isTerminator) {
          if(!isSampling) {
            windowStatistics.addEvent(event)
            costSlicerTracker.addEvent(event)
          }else{
            if(eventsObserved >= eventObsTillSample){
              eventsObserved = 0
              eventObsTillSample = Random.nextInt(sampleEventFreqUpperBoundary+1)+sampleEventFreqLowerBoundary
              windowStatistics.addEvent(event)
              costSlicerTracker.addEvent(event)
            }else{
              eventsObserved = eventsObserved + 1
            }
          }
        }
        if(isBuffering) {
          eventBuffer += event
        }else{
            c.collect(event)
        }
        if(doAdaptStuff && event.isTerminator) {
          if (shouldAdapt && !triggeredAdapt) {
            sendAdaptMessage(c,lastSlicing)
            triggeredAdapt = true
          } else {
            c.collect(Fact.meta("gapt", ""))
          }
          doAdaptStuff = false
          shouldAdapt = false
        }
        if(windowStatistics.hadRollover() && !event.isTerminator) {
          if(!isBuffering){
            makeAdaptDecision(c)
          }
          var prevSliceCandidate = sliceCandidate
          sliceCandidate = getSlicingStrategy(windowStatistics)
          if(prevSliceCandidate.toString.equals(sliceCandidate.toString)) {
            //the longer we'd want to switch to the same new canddiate, the more we assume that that it will continue to be optimal
            assumedFutureStableWindowsAmount = assumedFutureStableWindowsAmount + 1
          }else{
            //slow increase, doubling decrease mechanism taken from TCP's adaptive approach
            //we drastically penalize changing
            assumedFutureStableWindowsAmount = assumedFutureStableWindowsAmount / 2
          }
          if(!isBuffering) {
            costSlicerTracker.reset(sliceCandidate,lastSlicing)
          }else {
            makeAdaptDecision(c)
            for (v <- eventBuffer) {
              c.collect(v)
            }
            eventBuffer.clear()
          }
        }
        /*        if(!isBuffering)
                {
                  c.collect(event)
        /*          if(!event.isEndMarker) {
                    tempF2.write(event.toMonpoly + "\n")
                    tempF2.flush()
                  }
        */      }*/
      }


  }

  def startup(): Unit = {
    if(triggeredAdapt) {
      adaptComplete = true
      handleReopenTime()
    }
  }

  def shutdown(): Unit = {
    if(started) {
      logFile.close()
      logFile = null
    }
    started = false
    lastShutdownInitiatedTime = System.currentTimeMillis()
  }
}


