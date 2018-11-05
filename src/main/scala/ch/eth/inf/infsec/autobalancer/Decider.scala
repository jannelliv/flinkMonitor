package ch.eth.inf.infsec.autobalancer

import ch.eth.inf.infsec.trace.Record
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class DeciderFlatMap extends FlatMapFunction[Record,Record] {
  val windowStatistics = new WindowStatistics(150,0.0333333)
  val decisionMakingDelta = 1; //measured in events, todo: make param
  var dmdLeft = decisionMakingDelta
  var adaptationCost = 1000.0

  def costSlicing() : Double = {
    0
  }

  def costAdaptation() : Double = {
    adaptationCost
  }

  def emitRebalanceEvent() : Unit /*Record*/ = {
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
  }

  override def flatMap(event:Record,c:Collector[Record]): Unit = {
    if (!event.isEndMarker) {
      windowStatistics.addEvent(event)
    }
    dmdLeft = dmdLeft-1
    if(dmdLeft <= 0) {
      dmdLeft = decisionMakingDelta
      c.collect(event)
      //todo: figure out wether to rebalance and if so give instructions to rebalance
    }else {
      c.collect(event)
    }
  }
}

