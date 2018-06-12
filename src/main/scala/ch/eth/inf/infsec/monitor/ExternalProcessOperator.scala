package ch.eth.inf.infsec.monitor

import java.util
import java.util.concurrent.{LinkedBlockingQueue, Semaphore}

import ch.eth.inf.infsec.Processor
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.runtime.state.{KeyGroupRangeAssignment, StateInitializationContext, StateSnapshotContext}
import org.apache.flink.streaming.api.graph.StreamConfig
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator, Output}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.{LatencyMarker, StreamElement, StreamElementSerializer, StreamRecord}
import org.apache.flink.streaming.runtime.tasks.StreamTask
import org.apache.flink.util.ExceptionUtils

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

private trait PendingRequest

private trait PendingResult

private case class InputItem[IN](input: StreamRecord[IN]) extends PendingRequest

private case class OutputItem[OUT](output: StreamRecord[OUT]) extends PendingResult

private case class WatermarkItem(mark: Watermark) extends PendingRequest with PendingResult

private case class LatencyMarkerItem(marker: LatencyMarker) extends PendingRequest with PendingResult

private case class SnapshotRequestItem() extends PendingRequest

private case class SnapshotResultItem(snapshot: Array[Byte]) extends PendingResult

private case class ShutdownItem() extends PendingRequest with PendingResult


// TODO(JS): Limit the capacity of the resultQueue for backpressure towards the external process.
class ExternalProcessOperator[IN, PIN, POUT, OUT](
  partitionMapping: Int => Int,
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
  @transient private var outSerializer: StreamElementSerializer[OUT] = _
  @transient private var stateSerializer: TypeSerializer[Array[Byte]] = _

  @transient private var recoveredResults: ListState[StreamElement] = _
  @transient private var recoveredPreprocessing: ValueState[preprocessing.State] = _
  @transient private var recoveredPostprocessing: ValueState[postprocessing.State] = _
  @transient private var recoveredState: ValueState[Array[Byte]] = _

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
    resultLock = new Object
    resultQueue = new util.ArrayDeque[PendingResult]()
    snapshotReady = new Semaphore(0)
    outSerializer = new StreamElementSerializer[OUT](getOperatorConfig.getTypeSerializerOut(getUserCodeClassloader))

    val preprocessingType = TypeInformation.of(new TypeHint[preprocessing.State]() {})
    preprocessingSerializer = preprocessingType.createSerializer(getExecutionConfig)

    val postprocessingType = TypeInformation.of(new TypeHint[postprocessing.State]() {})
    postprocessingSerializer = postprocessingType.createSerializer(getExecutionConfig)

    val stateType: TypeInformation[Array[Byte]] = implicitly[TypeInformation[Array[Byte]]]
    stateSerializer = stateType.createSerializer(getExecutionConfig)
  }

  override def open(): Unit = {
    super.open()
    assert(Thread.holdsLock(taskLock))

    pendingCount = 0
    waitingRequest = null

    preprocessing.setParallelInstanceIndex(getRuntimeContext.getIndexOfThisSubtask)
    postprocessing.setParallelInstanceIndex(getRuntimeContext.getIndexOfThisSubtask)

    if (recoveredResults == null) {
      preprocessing.restoreState(None)
      postprocessing.restoreState(None)
      process.open()
    } else {
      assert(recoveredPreprocessing != null)
      assert(recoveredPostprocessing != null)
      assert(recoveredState != null)

      setKeyForPartition()

      preprocessing.restoreState(Some(recoveredPreprocessing.value()))
      postprocessing.restoreState(Some(recoveredPostprocessing.value()))

      for (element: StreamElement <- recoveredResults.get) {
        if (element.isRecord)
          resultQueue.add(OutputItem(element.asRecord()))
        else if (element.isWatermark)
          resultQueue.add(WatermarkItem(element.asWatermark()))
        else if (element.isLatencyMarker)
          resultQueue.add(LatencyMarkerItem(element.asLatencyMarker()))
        else
          throw new IllegalStateException("Unknown stream element type " + element.getClass +
            " encountered while opening the operator.")
      }

      process.open(recoveredState.value())

      recoveredPreprocessing = null
      recoveredResults = null
      recoveredState = null
    }

    writerThread = new ServiceThread {
      override def handle(): Unit = {
        val request = requestQueue.take()
        processingQueue.put(request)
        request match {
          case InputItem(record) => process.writeRequest(record.asRecord[PIN]().getValue)
          case WatermarkItem(_) => ()
          case LatencyMarkerItem(_) => ()
          case SnapshotRequestItem() => process.initSnapshot()
          case ShutdownItem() =>
            process.shutdown()
            running = false
        }
      }
    }
    writerThread.start()

    readerThread = new ServiceThread {
      private var buffer = new ArrayBuffer[POUT]()

      override def handle(): Unit = {
        val request = processingQueue.take()
        // FIXME(JS): Do more fine-grained locking; some of the calls below might be blocking!
        resultLock.synchronized {
          request match {
            case InputItem(record) =>
              try {
                process.readResults(buffer)
                for (result <- buffer)
                  postprocessing.process(result, r =>
                    resultQueue.add(OutputItem(outputRecord(record.asRecord(), r))))
                resultQueue.add(OutputItem(null))
              } finally {
                buffer.clear()
              }
            case mark@WatermarkItem(_) => resultQueue.add(mark)
            case marker@LatencyMarkerItem(_) => resultQueue.add(marker)
            case SnapshotRequestItem() =>
              resultQueue.add(SnapshotResultItem(process.readSnapshot()))
              snapshotReady.release()
            case shutdown@ShutdownItem() =>
              postprocessing.terminate(r =>
                resultQueue.add(OutputItem(new StreamRecord[OUT](r))))
              process.join()
              resultQueue.add(shutdown)
              running = false
          }
          resultLock.notifyAll()
        }
      }
    }
    readerThread.start()

    emitterThread = new ServiceThread {
      override def handle(): Unit = {
        resultLock.synchronized {
          while (resultQueue.isEmpty)
            resultLock.wait()
        }

        if (!running)
          return

        taskLock.synchronized {
          var result: PendingResult = null
          resultLock.synchronized {
            result = resultQueue.removeFirst()
          }

          // TODO(JS): In AsyncWaitOperator, a TimestampedCollector is used, which recycles a single record object.
          // Do we want to do the same here?
          result match {
            case OutputItem(record) =>
              if (record == null)
                pendingCount -= 1
              else
                output.collect(record.asRecord())
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

    setKeyForPartition()

    // Store the complete state: pending results and the process snapshot.
    val preprocessingState = getKeyedStateStore.getState(new ValueStateDescriptor[preprocessing.State](
      PREPROCESSING_STATE_NAME,
      preprocessingSerializer))
    val postprocessingState = getKeyedStateStore.getState(new ValueStateDescriptor[postprocessing.State](
      POSTPROCESSING_STATE_NAME,
      postprocessingSerializer))
    val resultState = getKeyedStateStore.getListState(new ListStateDescriptor[StreamElement](
      RESULT_STATE_NAME,
      outSerializer))
    val processState = getKeyedStateStore.getState(new ValueStateDescriptor[Array[Byte]](
      PROCESS_STATE_NAME,
      stateSerializer))

    preprocessingState.update(preprocessing.getState)
    postprocessingState.update(postprocessing.getState)

    resultState.clear()

    try {
      var gotSnapshot = false

      resultLock.synchronized {
        for (result: PendingResult <- resultQueue) {
          assert(!gotSnapshot)
          result match {
            case OutputItem(record) => resultState.add(record)
            case WatermarkItem(mark) => resultState.add(mark)
            case SnapshotResultItem(state) =>
              processState.update(state)
              gotSnapshot = true
            case ShutdownItem() => throw new Exception("Unexpected shutdown of process while taking snapshot.")
          }
        }
      }
      assert(gotSnapshot)
    } catch {
      case e: Exception =>
        resultState.clear()
        processState.clear()
        throw new Exception(
          "Could not add pending results to operator state backend of operator" +
            getOperatorName + ".", e)
    }
  }

  override def initializeState(context: StateInitializationContext): Unit = {
    super.initializeState(context)

    if (context.isRestored) {
      recoveredPreprocessing = getKeyedStateStore.getState(new ValueStateDescriptor[preprocessing.State](
        PREPROCESSING_STATE_NAME,
        preprocessingSerializer))
      recoveredPostprocessing = getKeyedStateStore.getState(new ValueStateDescriptor[postprocessing.State](
        POSTPROCESSING_STATE_NAME,
        postprocessingSerializer))
      recoveredResults = getKeyedStateStore.getListState(new ListStateDescriptor[StreamElement](
        RESULT_STATE_NAME,
        outSerializer))
      recoveredState = getKeyedStateStore.getState(new ValueStateDescriptor[Array[Byte]](
        PROCESS_STATE_NAME,
        stateSerializer))
    }
  }

  override def processWatermark(mark: Watermark): Unit = {
    enqueueRequest(WatermarkItem(mark))
  }

  override def processLatencyMarker(marker: LatencyMarker): Unit = {
    enqueueRequest(LatencyMarkerItem(marker))
  }

  override def processElement(streamRecord: StreamRecord[IN]): Unit = {
    // TODO(JS): Implement timeout
    preprocessing.process(streamRecord.getValue, x =>
      enqueueRequest(InputItem(new StreamRecord[PIN](x, streamRecord.getTimestamp))) )
  }

  def failOperator(throwable: Throwable): Unit = {
    getContainingTask.getEnvironment.failExternally(throwable)
  }

  // FIXME(JS): If the process dies, we might get stuck in taskLock.wait(), even though Flink is trying to cancel
  // the task.
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

  private def outputRecord(input: StreamRecord[IN], value: OUT): StreamRecord[OUT] =
    if (input.hasTimestamp)
      new StreamRecord[OUT](value, input.getTimestamp)
    else
      new StreamRecord[OUT](value)

  private def setKeyForPartition(): Unit = {
    val subtaskIndex = getContainingTask.getEnvironment.getTaskInfo.getIndexOfThisSubtask
    val key = partitionMapping(subtaskIndex)
    assert(KeyGroupRangeAssignment.assignKeyToParallelOperator(
      key,
      getContainingTask.getEnvironment.getTaskInfo.getMaxNumberOfParallelSubtasks,
      getContainingTask.getEnvironment.getTaskInfo.getNumberOfParallelSubtasks
    ) == subtaskIndex)

    setCurrentKey(key)
  }

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
  def transform[IN, K, PIN, POUT, OUT: TypeInformation](
      partitionMapping: Int => Int,
      in: KeyedStream[IN, K],
      preprocessing: Processor[IN, PIN],
      process: ExternalProcess[PIN, POUT],
      postprocessing: Processor[POUT, OUT],
      capacity: Int): DataStream[OUT] =
    in.transform(
      "External Process",
      new ExternalProcessOperator[IN, PIN, POUT, OUT](partitionMapping, preprocessing, process, postprocessing,0, capacity))
}
