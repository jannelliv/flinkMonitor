package ch.ethz.infsec.trace

import java.util
import collection.JavaConverters._

import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.trace.formatter.TraceFormatter
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.util.Collector

class PrintingFunction(var formatter: TraceFormatter)
  extends FlatMapFunction[Fact, String] with ListCheckpointed[TraceFormatter] {

  override def flatMap(fact: Fact, collector: Collector[String]): Unit = {
    if (!fact.isMeta)
      formatter.printFact(collector.collect(_), fact)
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[TraceFormatter] =
    util.Collections.singletonList(formatter)

  override def restoreState(state: util.List[TraceFormatter]): Unit = {
    //??? why this mark assert(state.size() == 0)
    formatter = state.get(0)
  }
}
