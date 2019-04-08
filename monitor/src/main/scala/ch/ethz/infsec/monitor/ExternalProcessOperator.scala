package ch.ethz.infsec.monitor

import java.util
import java.util.concurrent.{LinkedBlockingQueue, Semaphore}

import ch.ethz.infsec.Processor
import ch.ethz.infsec.slicer.HypercubeSlicer
import ch.ethz.infsec.trace.{CommandRecord, MonpolyVerdictFilter, Record}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.metrics.Gauge
import org.apache.flink.runtime.state.{StateInitializationContext, StateSnapshotContext}
import org.apache.flink.streaming.api.graph.StreamConfig
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator, Output}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.{LatencyMarker, StreamRecord}
import org.apache.flink.streaming.runtime.tasks.StreamTask
import org.apache.flink.util.ExceptionUtils

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

private trait PendingRequest

private trait PendingResult

private case class InputItem[IN](input: StreamRecord[IN]) extends PendingRequest

private case class OutputItem[OUT](output: StreamRecord[OUT]) extends PendingResult

private case class OutputSeparatorItem() extends PendingResult

private case class WatermarkItem(mark: Watermark) extends PendingRequest with PendingResult

private case class LatencyMarkerItem(marker: LatencyMarker) extends PendingRequest with PendingResult

private case class SnapshotRequestItem() extends PendingRequest

private case class SnapshotResultItem(snapshot: Iterable[(Int, Array[Byte])]) extends PendingResult

private case class ShutdownItem() extends PendingRequest with PendingResult


// TODO(JS): Limit the capacity of the resultQueue for backpressure towards the external process.
// TODO(JS): One could also consider removing the resultQueue and merging the reader and emitter threads. However, it
// is not obvious how the synchronization during snapshotting would work.
class ExternalProcessOperator[IN, PIN, POUT, OUT](
  slicer: HypercubeSlicer,
  preprocessing: Processor[IN, PIN],
  process: ExternalProcess[PIN, POUT],
  postprocessing: Processor[POUT, OUT],
  timeout: Long,
  capacity: Int)
  extends AbstractStreamOperator[OUT] with OneInputStreamOperator[IN, OUT] {

  require(capacity > 0)

  private val PREPROCESSING_STATE_NAME = "_external_process_operator_preprocessing_state_"
  private val POSTPROCESSING_STATE_NAME = "_external_process_operator_postprocessing_state_"
  private val RESULT_STATE_NAME = "_external_process_operator_result_state_"
  private val PROCESS_STATE_NAME = "_external_process_operator_process_state_"

  @transient private var taskLock: AnyRef = _

  @transient private var requestQueue: LinkedBlockingQueue[PendingRequest] = _
  @transient private var processingQueue: LinkedBlockingQueue[PendingRequest] = _
  @transient private var resultLock: Object = _
  @transient private var resultQueue: util.ArrayDeque[PendingResult] = _

  @transient private var snapshotReady: Semaphore = _

  @transient private var preprocessingSerializer: TypeSerializer[preprocessing.State] = _
  @transient private var postprocessingSerializer: TypeSerializer[postprocessing.State] = _
  @transient private var resultSerializer: TypeSerializer[Array[PendingResult]] = _
  @transient private var stateSerializer: TypeSerializer[(Int, Array[Byte])] = _

  @transient private var resultState: ListState[Array[PendingResult]] = _
  @transient private var preprocessingState: ListState[preprocessing.State] = _
  @transient private var postprocessingState: ListState[postprocessing.State] = _
  @transient private var processState: ListState[(Int,Array[Byte])] = _

  @transient private var pendingCount: Int = 0

  @transient private var waitingRequest: PendingRequest = _

  @transient private var writerThread: ServiceThread = _
  @transient private var readerThread: ServiceThread = _
  @transient private var emitterThread: ServiceThread = _

  override def setup(
    containingTask: StreamTask[_, _],
    config: StreamConfig,
    output: Output[StreamRecord[OUT]]): Unit = {

    super.setup(containingTask, config, output)
    taskLock = getContainingTask.getCheckpointLock
    requestQueue = new LinkedBlockingQueue[PendingRequest]()
    processingQueue = new LinkedBlockingQueue[PendingRequest]()
    resultLock = new Object()
    resultQueue = new util.ArrayDeque[PendingResult]()
    snapshotReady = new Semaphore(0)

    val preprocessingType = TypeInformation.of(new TypeHint[preprocessing.State]() {})
    preprocessingSerializer = preprocessingType.createSerializer(getExecutionConfig)

    val postprocessingType = TypeInformation.of(new TypeHint[postprocessing.State]() {})
    postprocessingSerializer = postprocessingType.createSerializer(getExecutionConfig)

    val pendingResultType = TypeInformation.of(new TypeHint[Array[PendingResult]]() {})
    resultSerializer = pendingResultType.createSerializer(getExecutionConfig)

    val stateType = TypeInformation.of(new TypeHint[(Int, Array[Byte])]() {})
    stateSerializer = stateType.createSerializer(getExecutionConfig)

    // TODO(JS): This is specific to the monitoring application.
    getMetricGroup.gauge[Long, Gauge[Long]]("numEvents", new Gauge[Long] {
      override def getValue: Long = preprocessing.getCustomCounter
    })
  }

  @transient
  var pendingSlicer: String = _

  override def open(): Unit = {
    super.open()
    assert(Thread.holdsLock(taskLock))

    pendingCount = 0
    waitingRequest = null

    val subtaskIndex = getRuntimeContext.getIndexOfThisSubtask
    preprocessing.setParallelInstanceIndex(subtaskIndex)
    process.identifier = Some(subtaskIndex.toString)

    val processStates = processState.get()
    preprocessing.restoreState(preprocessingState.get().headOption)
    postprocessing.restoreState(postprocessingState.get().headOption)

    val postProcessState = postprocessingState.get().headOption
    postprocessing.restoreState(postProcessState)
    postProcessState match {
      case Some(x) =>
        slicer.updateState(x.asInstanceOf[Array[Byte]])
        postprocessing.asInstanceOf[MonpolyVerdictFilter].updateProcessingFunction(slicer.mkVerdictFilter)
      case None =>
        postprocessing.asInstanceOf[MonpolyVerdictFilter].setCurrent(slicer.getState())
    }
    postprocessing.setParallelInstanceIndex(subtaskIndex)

    resultState.get().headOption match {
      case Some(results) =>
        for (item <- results)
          resultQueue.add(item)
      case None => ()
    }

    processStates.headOption match {
      case Some(_) =>
        val relevant = processStates.filter(_._1 == subtaskIndex)
        if(relevant.size == 1) process.open(relevant.head._2)
        else process.open(relevant)
      case None => process.open()
    }

    preprocessingState.clear()
    postprocessingState.clear()
    processState.clear()
    resultState.clear()

    writerThread = new ServiceThread {
      override def handle(): Unit = {
        val request = requestQueue.take()
        processingQueue.put(request)
        request match {
          case InputItem(record)   => process.writeRequest(record.asRecord[PIN]().getValue)
          case WatermarkItem(_) => ()
          case LatencyMarkerItem(_) => ()
          case SnapshotRequestItem() =>
            process.initSnapshot()
          case ShutdownItem() =>
            process.shutdown()
            running = false
        }
      }
    }
    writerThread.start()

    readerThread = new ServiceThread {
      private var buffer = new ArrayBuffer[POUT]()

      private def putResult(result: PendingResult): Unit = resultLock.synchronized {
        resultQueue.add(result)
        resultLock.notifyAll()
      }

      override def handle(): Unit = {
        val request = processingQueue.take()
        request match {
          case InputItem(record) =>
            try {
              process.readResults(buffer)
              resultLock.synchronized {
                for (result <- buffer)
                  postprocessing.process(
                    result, r => resultQueue.add(OutputItem(outputRecord(record, r))))
                resultQueue.add(OutputSeparatorItem())
                resultLock.notifyAll()
              }
            } finally {
              buffer.clear()
            }

          case mark@WatermarkItem(_) => putResult(mark)
          case marker@LatencyMarkerItem(_) => putResult(marker)
          case SnapshotRequestItem() =>
            val results = process.readSnapshots()
            if(results.size == 1){
              val result = results.head
              putResult(SnapshotResultItem(List((subtaskIndex, result._2))))
            }
            else
              putResult(SnapshotResultItem(results))

            snapshotReady.release()
          case shutdown@ShutdownItem() =>
            process.drainResults(buffer)

            resultLock.synchronized {
              for (result <- buffer)
                postprocessing.process(
                  result, r =>
                    resultQueue.add(OutputItem(new StreamRecord[OUT](r))))
              postprocessing.terminate(r =>
                resultQueue.add(OutputItem(new StreamRecord[OUT](r))))
              resultQueue.add(OutputSeparatorItem())
              resultLock.notifyAll()
            }
            process.join()
            putResult(shutdown)
            running = false
        }
      }
    }
    readerThread.start()

    emitterThread = new ServiceThread {
      override def handle(): Unit = {
        var result = null
        resultLock.synchronized {
          while (resultQueue.isEmpty)
            resultLock.wait()
        }

        taskLock.synchronized {
          var result: PendingResult = null
          resultLock.synchronized {
            if (resultQueue.isEmpty)
              return
            result = resultQueue.removeFirst()
          }
          if (result == null)
            return

          // TODO(JS): In AsyncWaitOperator, a TimestampedCollector is used, which recycles a single record object.
          // Do we want to do the same here?
          result match {
            case OutputItem(record) => output.collect(record.asRecord())
            case OutputSeparatorItem() => pendingCount -= 1
            case WatermarkItem(mark) =>
              output.emitWatermark(mark)
              pendingCount = -1
            case LatencyMarkerItem(marker) =>
              latencyStats.reportLatency(marker)
              output.emitLatencyMarker(marker)
              pendingCount = -1
            case SnapshotResultItem(state) => ()
            case ShutdownItem() => running = false
          }

          taskLock.notifyAll()
        }
      }
    }
    emitterThread.start()
  }

  override def close(): Unit = {
    try {
      preprocessing.terminate(x => enqueueRequest(InputItem(new StreamRecord[PIN](x))))

      requestQueue.put(ShutdownItem())

      writerThread.join()
      readerThread.join()

      if (Thread.holdsLock(taskLock)) {
        while (emitterThread.isAlive)
          taskLock.wait(100L)
      }
      emitterThread.join()
    } catch {
      // TODO(JS): Not sure whether this correct.
      case _: InterruptedException => Thread.currentThread().interrupt()
    }

    super.close()
  }

  override def dispose(): Unit = {
    if (writerThread != null)
      writerThread.interruptAndStop()
    if (readerThread != null)
      readerThread.interruptAndStop()
    if (emitterThread != null)
      emitterThread.interruptAndStop()

    var exception: Exception = null

    try {
      if (process != null)
        process.dispose()
    } catch {
      case e: InterruptedException =>
        exception = e
        Thread.currentThread().interrupt()
      case e: Exception => exception = e
    }

    try {
      super.dispose()
    } catch {
      case e: InterruptedException =>
        exception = ExceptionUtils.firstOrSuppressed(e, exception)
        Thread.currentThread().interrupt()
      case e: Exception => exception = ExceptionUtils.firstOrSuppressed(e, exception)
    }

    if (exception != null)
      throw exception
  }

  override def snapshotState(context: StateSnapshotContext): Unit = {
    super.snapshotState(context)
    assert(Thread.holdsLock(taskLock))

    // TODO(JS): Ideally, all of this would happen in the asynchronous part of the snapshot.

    // We trigger the snapshot request immediately, even if the number of pending requests/results is at capacity.
    // In particular, we _must not_ release the checkpointing lock, which would allow the emitter thread to
    // send pending results downstream. Since the barrier has already been sent to downstream operators at
    // this point, this would result in inconsistent snapshots.

    if (waitingRequest != null) {
      requestQueue.put(waitingRequest)
      pendingCount += 1
      waitingRequest = null
    }
    requestQueue.put(SnapshotRequestItem())

    // Wait until the process snapshot has been created.
    snapshotReady.acquire()
    assert(snapshotReady.availablePermits() == 0)
    assert(requestQueue.isEmpty)

    // Store the complete state: pending results and the process snapshot.

    preprocessingState.clear()
    preprocessingState.add(preprocessing.getState)
    postprocessingState.clear()
    postprocessingState.add(postprocessing.getState)

    resultState.clear()
    processState.clear()

    try {
      var gotSnapshot = false
      val results = new ArrayBuffer[PendingResult]()

      resultLock.synchronized {
        for (result: PendingResult <- resultQueue) {
          gotSnapshot = false
          result match {
            case OutputItem(_) => results.add(result)
            case OutputSeparatorItem() => results.add(result)
            case WatermarkItem(_) => results.add(result)
            case LatencyMarkerItem(_) => results.add(result)
            case SnapshotResultItem(states) =>
              processState.addAll(states.toList)
              gotSnapshot = true
            case ShutdownItem() => throw new Exception("Unexpected shutdown of process while taking snapshot.")
          }
        }
      }
      assert(gotSnapshot)
      resultState.add(results.toArray)
    } catch {
      case e: Exception =>
        throw new Exception(
          "Could not add pending results to operator state backend of operator" +
            getOperatorName + ".", e)
    }
  }

  override def initializeState(context: StateInitializationContext): Unit = {
    super.initializeState(context)

    preprocessingState = getOperatorStateBackend.getUnionListState(new ListStateDescriptor[preprocessing.State](
      PREPROCESSING_STATE_NAME,
      preprocessingSerializer))
    postprocessingState = getOperatorStateBackend.getUnionListState(new ListStateDescriptor[postprocessing.State](
      POSTPROCESSING_STATE_NAME,
      postprocessingSerializer))
    resultState = getOperatorStateBackend.getUnionListState(new ListStateDescriptor[Array[PendingResult]](
      RESULT_STATE_NAME,
      resultSerializer))
    processState = getOperatorStateBackend.getUnionListState(new ListStateDescriptor[(Int, Array[Byte])](
      PROCESS_STATE_NAME,
      stateSerializer))
  }

  override def processWatermark(mark: Watermark): Unit = {
    enqueueRequest(WatermarkItem(mark))
  }

  override def processLatencyMarker(marker: LatencyMarker): Unit = {
    enqueueRequest(LatencyMarkerItem(marker))
  }

  override def processElement(streamRecord: StreamRecord[IN]): Unit = {
    // TODO(JS): Implement timeout
    val tuple = streamRecord.getValue.asInstanceOf[(Int, Record)]
    tuple._2 match {
      case CommandRecord(_, parameters) =>
        postprocessing.asInstanceOf[MonpolyVerdictFilter].updatePending(parameters.toCharArray.map(_.toByte))
        pendingSlicer = parameters
      case _ => ()
    }

    preprocessing.process(streamRecord.getValue, x =>
      enqueueRequest(InputItem(new StreamRecord[PIN](x, streamRecord.getTimestamp))))
  }

  def failOperator(throwable: Throwable): Unit = {
    getContainingTask.getEnvironment.failExternally(throwable)
  }

  private def enqueueRequest(request: PendingRequest): Unit = {
    assert(Thread.holdsLock(taskLock))
    waitingRequest = request
    while (waitingRequest != null && pendingCount >= capacity)
      taskLock.wait()
    if (waitingRequest != null) {
      requestQueue.put(waitingRequest)
      pendingCount += 1
      waitingRequest = null
    }
  }

  private def outputRecord[T](input: StreamRecord[T], value: OUT): StreamRecord[OUT] =
    if (input.hasTimestamp)
      new StreamRecord[OUT](value, input.getTimestamp)
    else
      new StreamRecord[OUT](value)

  abstract private class ServiceThread extends Thread {
    @volatile var running = true

    def handle(): Unit

    override def run(): Unit = {
      try {
        while (running)
          handle()
      } catch {
        case e: InterruptedException =>
          if (running)
            failOperator(e)
        case e: Throwable =>
          failOperator(e)
      }
    }

    def interruptAndStop(): Unit = {
      running = false
      interrupt()
    }
  }

}

object ExternalProcessOperator {
  // TODO(JS): Do we need to "clean" the process/preprocessing?
  def transform[IN, PIN, POUT, OUT: TypeInformation](
      slicer: HypercubeSlicer,
      in: DataStream[(Int, Record)],
      preprocessing: Processor[(Int, Record), PIN],
      process: ExternalProcess[PIN, POUT],
      postprocessing: Processor[POUT, OUT],
      capacity: Int): DataStream[OUT] =
    in.transform(
      "External Process",
      new ExternalProcessOperator[(Int, Record), PIN, POUT, OUT](slicer, preprocessing, process, postprocessing,0, capacity))
}
