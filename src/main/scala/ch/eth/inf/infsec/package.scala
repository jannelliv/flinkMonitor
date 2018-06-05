package ch.eth.inf

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

package object infsec {

  trait Processor[I, O] {
    type State

    def isStateful: Boolean = true
    def getState: State
    def restoreState(state: Option[State])

    def process(in: I, f: O => Unit)
    def terminate(f: O => Unit)

    def processAll(in: TraversableOnce[I]): IndexedSeq[O] = {
      val buffer = new ArrayBuffer[O]()
      in.foreach(process(_, buffer.append(_)))
      terminate(buffer.append(_))
      buffer
    }
  }

  trait StatelessProcessor[I, O] extends Processor[I, O] {
    override type State = Unit

    override def isStateful: Boolean = false
    override def getState: State = ()
    override def restoreState(state: Option[Unit]) {}
  }

  // TODO(JS): Call Processor#terminate if the stream has ended (via RichFlatMapFunction#close).
  // TODO(JS): Not sure whether this state management logic is sound ...
  class ProcessorFunction[I, O](processor: Processor[I, O] with Serializable)
    extends FlatMapFunction[I, O] with CheckpointedFunction {

    @transient
    private var checkpointedState: ListState[processor.State] = _

    override def flatMap(t: I, collector: Collector[O]): Unit = processor.process(t, collector.collect)

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      if (processor.isStateful) {
        checkpointedState.clear()
        checkpointedState.add(processor.getState)
      }
    }

    override def initializeState(context: FunctionInitializationContext) {
      if (processor.isStateful) {
        val descriptor = new ListStateDescriptor[processor.State](
          "state",
          TypeInformation.of(new TypeHint[processor.State] {}))

        checkpointedState = context.getOperatorStateStore.getListState(descriptor)
        if (context.isRestored) {
          val stateIterator = checkpointedState.get().iterator()
          val state = if (stateIterator.hasNext) Some(stateIterator.next()) else None
          processor.restoreState(state)
        }
      }
    }
  }

}
