package ch.ethz.infsec.tools

import ch.ethz.infsec.monitor.Fact
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction

class FaultInjector(trigger: Int) extends RichMapFunction[Fact, Fact] with CheckpointedFunction {
  @transient private var counter: Int = _
  @transient private var recovered: Boolean = _

  override def map(value: Fact): Fact = {
    if (recovered || counter < trigger) {
      if (!value.isMeta && !value.isTerminator) counter += 1
      value
    } else {
      throw new Exception("Injected fault")
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = ()

  override def initializeState(context: FunctionInitializationContext): Unit = {
    counter = 0
    recovered = context.isRestored || getRuntimeContext.getIndexOfThisSubtask != 0
  }
}
