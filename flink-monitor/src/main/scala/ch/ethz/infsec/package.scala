package ch.ethz

import java.io.FileWriter

import ch.ethz.infsec.monitor.{ExternalProcessFactory, MonitorRequest, MonitorResponse}
import ch.ethz.infsec.trace.Record
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

package object infsec {

  trait Processor[I, +O] {
    type State

    def isStateful: Boolean = true

    /**
      * Obtain a view on the current state of this processor. The returned value must not be modified anymore!
      *
      * @return the current state of this processor
      */
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

    // TODO(JS): Leaky abstraction
    def setParallelInstanceIndex(instance: Int): Unit = ()
    def getCustomCounter: Long = 0
  }

  trait StatelessProcessor[I, +O] extends Processor[I, O] {
    override type State = Unit

    override def isStateful: Boolean = false
    override def getState: State = ()
    override def restoreState(state: Option[Unit]) {}
  }

  object StatelessProcessor {
    def identity[T]: StatelessProcessor[T, T] = new StatelessProcessor[T, T] with Serializable {
      override def process(in: T, f: T => Unit): Unit = f(in)

      override def terminate(f: T => Unit): Unit = ()
    }
  }

  // TODO(JS): We would like to call processor.terminate(...) if the stream has ended, but where do we
  // get the collector from?
  class ProcessorFunction[I, O](processor: Processor[I, O] with Serializable)
    extends RichFlatMapFunction[I, O] with CheckpointedFunction {

    @transient
    private var checkpointedState: ListState[processor.State] = _


    var logFile : FileWriter = _
    var started = false

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      val index = getRuntimeContext.getIndexOfThisSubtask
      println("Opening %s instance %d".format(getRuntimeContext.getTaskName, getRuntimeContext.getIndexOfThisSubtask))
      //TODO(FB): discuss with JS
      //if (index > 0 && processor.isStateful)
      //  throw new Exception("Stateful processors cannot be used in parallel.")
      processor.setParallelInstanceIndex(index)
      if(!started) {
        started = true
        logFile = new FileWriter("ProcFunc.log", true)
        logFile.write("started\n")
      }
      logFile.write("open\n")
      logFile.flush()
    }

    override def flatMap(t: I, collector: Collector[O]): Unit = processor.process(t, collector.collect)

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      if(!started) {
        started = true
        logFile = new FileWriter("ProcFunc.log", true)
        logFile.write("started\n")
      }
      logFile.write("snapshotState\n")
      logFile.flush()
      if (processor.isStateful) {
        checkpointedState.clear()
        checkpointedState.add(processor.getState)
      }
    }

    override def initializeState(context: FunctionInitializationContext) {
      if(!started) {
        started = true
        logFile = new FileWriter("ProcFunc.log", true)
        logFile.write("started\n")
      }
      logFile.write("initializeState\n")
      logFile.flush()
      if (processor.isStateful) {
        val descriptor = new ListStateDescriptor[processor.State](
          "state",
          TypeInformation.of(new TypeHint[processor.State] {}))

        checkpointedState = context.getOperatorStateStore.getUnionListState(descriptor)
        if (context.isRestored) {
          val state =  checkpointedState.get().asScala.headOption
          processor.restoreState(state)
        }
      }
    }
  }

  type MonitorFactory = ExternalProcessFactory[(Int, Record), MonitorRequest, MonitorResponse, MonitorResponse]
}
