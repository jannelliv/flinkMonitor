package ch.eth.inf.infsec.autobalancer

import ch.eth.inf.infsec.trace.Record
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class DeciderFlatMap extends FlatMapFunction[Record,Record] {
  val windowStatistics = new WindowStatistics
  val decisionMakingDelta = 10000; //measured in events, todo: make param
  var dmdLeft = decisionMakingDelta

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

