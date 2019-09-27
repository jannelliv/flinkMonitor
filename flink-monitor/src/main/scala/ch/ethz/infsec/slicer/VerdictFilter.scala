package ch.ethz.infsec.slicer

import java.util

import ch.ethz.infsec.monitor.Fact
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed

// TODO(JS): There is some code duplication with DataSlicer.
// FIXME(JS): It's unclear whether sharing the slicer object is a problem if its state is mutated.
class VerdictFilter(val slicer: DataSlicer) extends RichFilterFunction[Fact] with ListCheckpointed[String] {
  var pendingSlicer: String = _

  def setSlicer(fact: Fact): Unit = {
    pendingSlicer = fact.getArgument(0).asInstanceOf[String]
  }

  override def filter(fact: Fact): Boolean = {
    if (fact.isMeta) {
      if (fact.getName == "set_slicer") {
        setSlicer(fact)
      }
      true
    } else if (fact.isTerminator) {
      true
    } else {
      slicer.filterVerdict(getRuntimeContext.getIndexOfThisSubtask, fact)
    }
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[String] =
    util.Collections.singletonList(if (pendingSlicer == null) slicer.stringify else pendingSlicer)

  override def restoreState(state: util.List[String]): Unit = {
    assert(state.size() == 1)
    slicer.unstringify(state.get(0))
  }
}
