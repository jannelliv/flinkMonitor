package ch.ethz.infsec.trace

import java.util

import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.trace.parser.TraceParser
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.util.Collector

class ParsingFunction(var parser: TraceParser) extends FlatMapFunction[String, Fact] with ListCheckpointed[TraceParser] {
  override def flatMap(line: String, collector: Collector[Fact]): Unit = parser.parseLine(collector.collect(_), line)

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[TraceParser] =
    util.Collections.singletonList(parser)

  override def restoreState(state: util.List[TraceParser]): Unit = {
    assert(state.size() == 1)
    parser = state.get(0)
  }
}
