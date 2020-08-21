package ch.ethz.infsec.autobalancer

import java.io.FileWriter
import java.util

import ch.ethz.infsec.autobalancer.HypercubeCostSlicerTracker.SlicerTrackerCost
import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.policy.Formula
import ch.ethz.infsec.slicer.HypercubeSlicer
import ch.ethz.infsec.tools.Rescaler.RescaleInitiator
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

//problem: how to solve "final bit of stream needs to be processed to"
//proposed solution: "end of stream message"
//TODO: properly parameterize
class AllState(_rfmf : DeciderFlatMapSimple)
  extends FlatMapFunction[(Int, Fact), Fact] with ListCheckpointed[DeciderFlatMapSimple] with Serializable
{
  var rfmf : DeciderFlatMapSimple = _rfmf
  type State = DeciderFlatMapSimple

  var started = true

  private def getState: DeciderFlatMapSimple = {
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

  override def flatMap(in: (Int, Fact), c: Collector[Fact]): Unit = {
    rfmf.flatMap(in, c)
  }

  def terminate(f: Fact => Unit): Unit = {
  }
}

abstract class DeciderFlatMap
                (degree : Int, numSources: Int,
                 shouldDumpData : Boolean)
  extends FlatMapFunction[(Int, Fact), Fact] with Serializable {

  def firstSlicing : HypercubeSlicer
  def getSlicingStrategy(ws : StatsHistogram) : HypercubeSlicer
  def slicingCost(strat:HypercubeSlicer, events:ArrayBuffer[Fact]) : Double
  def adaptationCost(strat:HypercubeSlicer, ws:StatsHistogram) : Double

  var avgMaxProcessingTime : Double = 0.01

  var lastSlicing: HypercubeSlicer = firstSlicing
  var sliceCandidate: HypercubeSlicer = lastSlicing
  var eventBuffer: ArrayBuffer[Fact] = ArrayBuffer[Fact]()

  var shutdownTime : Double = 1000.0

  var assumedFutureStableWindowsAmount : Double = 1.0

  var isAdapting = false
  var slicingCosts: Option[HypercubeCostSlicerTracker] = None
  var histogram: Option[StatsHistogram] = None
  var noHistograms: Int = 0
  var noCost: Int = 0
  var noApt: Int = 0
  var waitingForCosts = false
  var waitingForHistograms = false
  var waitingForApts = false
  var waitingForAdaptConfirms = false
  var noAdaptConfirms = 0;

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

  def sendAdaptMessage(c: Collector[Fact], strat: HypercubeSlicer) : Unit = {
    val param:String = strat.stringify
    c.collect(Fact.meta("set_slicer", param))
    waitingForAdaptConfirms = true
  }

  def makeAdaptDecision(costFirst: Int, costSecond: Int) : Unit = {
    val prevSlicerForLog = lastSlicing
    //while in the middle of an adapting we don't re-adapt
    val lhs = costFirst * avgMaxProcessingTime + adaptationCost(sliceCandidate,histogram.get) / assumedFutureStableWindowsAmount
    val rhs = costSecond * avgMaxProcessingTime
    if(lhs < rhs) {
      lastSlicing = sliceCandidate
      isAdapting = true
    }
    if(shouldDumpData) {
      val realtimeTimestamp = System.currentTimeMillis()
      logFile.write("--------- Status at time: "+realtimeTimestamp+" ---------\n")
      logFile.write(s"lhs is: $lhs, rhs is: $rhs\n")
      logFile.write("adapting: "+isAdapting+"\n")
      logFile.write("current slicer: "+prevSlicerForLog+"\n")
      if(isAdapting) {
        logFile.write("new slicer: "+sliceCandidate.toString+"\n")
      }else{
        logFile.write("rejected slicer candidate: "+sliceCandidate+"\n")
      }
      logFile.write("cost for current: "+costSecond+" units\n")
      logFile.write("cost for new: "+costFirst+" units\n")
      logFile.write("average max processing time: "+avgMaxProcessingTime+" ms/unit\n")
      logFile.write("predicted stability: "+assumedFutureStableWindowsAmount+" windows\n")
      //logFile.write("shutdown memory: "+shutdownMemory+" kB\n")
      logFile.write("shutdown time: "+shutdownTime+" ms\n")
      logFile.write("----- End Status at time: "+realtimeTimestamp+" -----\n")
      logFile.flush()
    }
  }

  //var shutdownMemory : Long = 0

  @transient  var logFile : FileWriter = _
  def logCommand(partition: Int, com:String,params:Any): Unit = {
    if(shouldDumpData) {
          logFile.write(s"==== RECEIVED COMMAND FROM PARTITION $partition: $com $params ====\n")
      logFile.flush()
    }
  }

  @transient  var started = false
  //  var tempF : FileWriter = _
  //  var tempF2 : FileWriter = _
  override def flatMap(in: (Int, Fact), c: Collector[Fact]): Unit = {
    /*if (isAdapting)
      throw new RuntimeException("shouldAdapt == true ==> no more commands")*/
    val event = in._2
    if(!event.isMeta) {
      throw new RuntimeException("decider expects meta facts")
    }
    val eventPartition = in._1
    if(!started) {
      started = true
      logFile = new FileWriter("decider.log",true)
      logFile.write("started\n")
      logFile.flush()
      c.collect(Fact.meta("init_slicer_tracker", lastSlicing.stringify))
      waitingForCosts = true
      logFile.write("waiting for costs = true\n")
      waitingForHistograms = true
      isAdapting = false
      logFile.write("waiting for histogram = true\n")
    }
    val com = event.getName
    if(com == "apt") {
      if (!waitingForApts)
        throw new RuntimeException("INVARIANT: receive apt ==> waitingForApts == true")
      val apt = Double.unbox(event.getArgument(0))
      logCommand(eventPartition, com, apt)
      avgMaxProcessingTime = math.max(apt.toDouble / 1000.0, avgMaxProcessingTime)
      if(avgMaxProcessingTime < 0.0001)
        avgMaxProcessingTime = 0.0001
      noApt += 1
      if (noApt > degree)
        throw new RuntimeException("INVARIANT: noApt <= degree")
      if (noApt == degree) {
        noApt = 0
        waitingForApts = false
        logFile.write("waiting for apts = false\n")
      }
    } else if(com == "memory") {
      //shutdownMemory = event.getArgument(0).asInstanceOf[java.lang.String].toLong
      //logCommand(eventPartition, com, shutdownMemory)
    } else if(com == "slicing_cost") {
      logCommand(eventPartition, "slicing_cost", "")
      if (!waitingForCosts)
        throw new RuntimeException("INVARIANT: receive slicing_cost ==> waitingForCosts == true")
      val newCosts = HypercubeCostSlicerTracker.fromBase64(event.getArgument(0).asInstanceOf[java.lang.String])
      slicingCosts match {
        case None =>
          if (noCost != 0)
            throw new RuntimeException("INVARIANT: slicingCosts == NONE ==> noCosts == 0")
          slicingCosts = Some(newCosts)
        case Some(costs) =>
          if (noCost <= 0)
            throw new RuntimeException("INVARIANT: histogram == Some(_) ==> noCosts > 0")
          costs.merge(newCosts)
      }
      noCost += 1
      if (noCost > numSources)
        throw new RuntimeException("INVARIANT: noCost <= numSources")
      if (noCost == numSources) {
        noCost = 0
        waitingForCosts = false
        logFile.write("waiting for costs = false\n")
      }
    } else if(com == "histogram") {
      logCommand(eventPartition, "histogram", "")
      if (!waitingForHistograms)
        throw new RuntimeException("INVARIANT: receive histogram ==> waitingForHistograms == true")
      val newHistogram = StatsHistogram.fromBase64(event.getArgument(0).asInstanceOf[java.lang.String])
      histogram match {
        case None =>
          if (noHistograms != 0)
            throw new RuntimeException("INVARIANT: histogram == NONE ==> noReceived == 0")
          histogram = Some(newHistogram)
        case Some(h) =>
          if (noHistograms <= 0)
            throw new RuntimeException("INVARIANT: histogram == Some(_) ==> noReceived > 0")
          h.merge(newHistogram)
      }
      noHistograms += 1
      if (noHistograms > numSources)
        throw new RuntimeException("INVARIANT: noHistograms <= numSources")
      if (noHistograms == numSources) {
        noHistograms = 0
        waitingForHistograms = false
        logFile.write("waiting for histogram = false\n")
      }
    } else if (com == "hello_decider") {
      return
    } else if (com == "set_slicer") {
      if (!waitingForAdaptConfirms)
        throw new RuntimeException("INVARIANT: set_slicer ==> waitingforadaptconfirms")
      noAdaptConfirms += 1
      logFile.write(s"got total of $noAdaptConfirms adapt confirmations")
      if (noAdaptConfirms == degree) {
        noAdaptConfirms = 0
        waitingForAdaptConfirms = false
        logFile.write("now triggering rescaling")
        new RescaleInitiator().rescale(degree)
      }
    } else {
      throw new Exception("META FACT UNKNOWN TO THE DECIDER")
    }

    if(waitingForHistograms || waitingForCosts || waitingForApts || waitingForAdaptConfirms)
      return

    val costs = slicingCosts.get.bothCosts()
    makeAdaptDecision(costs._1, costs._2)
    if (isAdapting) {
      sendAdaptMessage(c,lastSlicing)
    } else {
      val prevSliceCandidate = sliceCandidate
      sliceCandidate = getSlicingStrategy(histogram.get)
      if(prevSliceCandidate.toString.equals(sliceCandidate.toString)) {
        //the longer we'd want to switch to the same new canddiate, the more we assume that that it will continue to be optimal
        assumedFutureStableWindowsAmount = assumedFutureStableWindowsAmount + 1
      }else{
        //slow increase, doubling decrease mechanism taken from TCP's adaptive approach
        //we drastically penalize changing
        assumedFutureStableWindowsAmount = assumedFutureStableWindowsAmount / 2
      }
      noCost = 0
      noHistograms = 0
      slicingCosts = None
      histogram = None
      c.collect(Fact.meta("start_sampling", sliceCandidate.stringify))
      waitingForHistograms = true
      logFile.write("waiting for histogram = true\n")
      waitingForCosts = true
      logFile.write("waiting for costs = true\n")
      c.collect(Fact.meta("get_apt", ""))
      waitingForApts = true
      logFile.write("waiting for apts = true\n")
    }
  }

  def startup(): Unit = {
    isAdapting = false
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


class DeciderFlatMapSimple(degree : Int, numSources:Int, formula : Formula) extends DeciderFlatMap(degree,numSources,true) {
  override def firstSlicing: HypercubeSlicer = {
    HypercubeSlicer.optimize(
      formula, degree, ConstantHistogram())
  }
  override def getSlicingStrategy(ws : StatsHistogram) : HypercubeSlicer = {
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

  //the cost of adaptation consists essentially of two parts
  //1. the cost of disturbing the current operation for a rebalance
  //2. the cost of shuffling all the data around for the actual rebalance

  //estimating either one is difficult, 1 entirely depends on the performance of the hypercube slicer for making a new slicing and the framework for dealing with a change of flow
  //ideally though 1 is negligible
  //the cost of shuffling all the data around is also hard to estimate, as it deals with networks as well as the details of the monitoring algo
  //we can assume that the cost of adaptation is at most linear in the total amount of memory used by all nodes (or rather O(n*m) at worst, with n being amount of nodes and m being amount of memory, which would be all nodes transmitting all their memory to all other nodes)
  override def adaptationCost(strat: HypercubeSlicer, ws: StatsHistogram): Double = shutdownTime
}