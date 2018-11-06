package ch.eth.inf.infsec.autobalancer

import ch.eth.inf.infsec.trace.Record
import ch.eth.inf.infsec.trace.Tuple
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector


class DeciderFlatMap[SlicingStrategy](firstSlicing : SlicingStrategy, getSlicingStrategy : WindowStatistics => SlicingStrategy, slicingCost : (SlicingStrategy,WindowStatistics) => Double, adaptationCost : (SlicingStrategy,WindowStatistics) => Double) extends FlatMapFunction[Record,Record] {
  val windowStatistics = new WindowStatistics(150,0.0333333)
  val decisionMakingDelta = 1; //measured in events, todo: make param
  var dmdLeft = decisionMakingDelta
  var oldSlicingStrategy : SlicingStrategy = firstSlicing
/*  var adaptationCost = 1000.0

  def costSlicing(strategy : SlicingStrategy) : Double = {
    0
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
  }

  def costAdaptation() : Double = {
    adaptationCost
    //the cost of adaptation consists essentially of two parts
    //1. the cost of disturbing the current operation for a rebalance
    //2. the cost of shuffling all the data around for the actual rebalance

    //estimating either one is difficult, 1 entirely depends on the performance of the hypercube slicer for making a new slicing and the framework for dealing with a change of flow
    //ideally though 1 is negligible
    //the cost of shuffling all the data around is also hard to estimate, as it deals with networks as well as the details of the monitoring algo
    //we can assume that the cost of adaptation is at most linear in the total amount of memory used by all nodes (or rather O(n*m) at worst, with n being amount of nodes and m being amount of memory, which would be all nodes transmitting all their memory to all other nodes)
  }*/

  //we can in theory decouple the details of the decider framework from the details of the cost functions
  //thereby allowing us to reuse the same framework as we change the functions
  //it also should make the implementation nicer


  //if need be, we can split the optimization algorithm up, such that instead of doing all computations at the point where it is triggered
  //it is doing them over the next N events. This may collide with a recalculating by the windowStatistics, but since correctness of the monitoring is not affected, this at worst gives a temporary misscalculation of what the most beneficial thing may be (due to parts of the calculation using more up to date data than other parts)

  def emitRebalanceEvent(slicingStrategy: SlicingStrategy) : Record = {
    //todo: complete
      val total:Double = windowStatistics.relations.foldRight(0)((p,acc)  => p._2+acc)
      var ratesString = ""
      for(p <- windowStatistics.relations) {
        ratesString += p._1 + "=" + (p._2 / total).toString() + ","
      }
      var heavyString = ""
      for(p <- windowStatistics.heavyHitter)
      {
        for(u <- p._2) {
          heavyString += p._1._1 + "," + p._1._2.toString() + "," + u.toString()
        }
      }
      Record(0,"",Tuple())
  }

  override def flatMap(event:Record,c:Collector[Record]): Unit = {
    if (!event.isEndMarker) {
      windowStatistics.addEvent(event)
    }
    dmdLeft = dmdLeft-1
    if(dmdLeft <= 0) {
      dmdLeft = decisionMakingDelta
      c.collect(event)
      var slicestrat = getSlicingStrategy(windowStatistics)
      if(slicestrat != oldSlicingStrategy) {
        if(slicingCost(slicestrat,windowStatistics)+adaptationCost(slicestrat,windowStatistics)
          < slicingCost(oldSlicingStrategy,windowStatistics)) {
          oldSlicingStrategy = slicestrat
          c.collect(emitRebalanceEvent(slicestrat))
        }
      }
    }else {
      c.collect(event)
    }
  }
}

