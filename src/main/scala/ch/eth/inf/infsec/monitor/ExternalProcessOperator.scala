package ch.eth.inf.infsec.monitor

import java.nio.file.{Files, Path}
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
import org.apache.flink.streaming.runtime.streamrecord.{StreamElement, StreamElementSerializer, StreamRecord}
import org.apache.flink.streaming.runtime.tasks.StreamTask
import org.apache.flink.util.ExceptionUtils

import scala.collection.JavaConversions._

private trait PendingRequest

private trait PendingResult

private case class InputItem[IN](input: StreamRecord[IN]) extends PendingRequest

private case class OutputItem[OUT](output: StreamRecord[OUT]) extends PendingResult

private case class WatermarkItem(mark: Watermark) extends PendingRequest with PendingResult

private case class SnapshotRequestItem() extends PendingRequest

private case class SnapshotResultItem(snapshot: Array[Byte]) extends PendingResult

private case class RestoreItem(snapshot: Array[Byte]) extends PendingRequest

private case class ShutdownItem() extends PendingRequest with PendingResult


class ExternalProcessOperator[IN, PIN, OUT](
  partitionMapping: Int => Int,
  preprocessing: Processor[IN, PIN],
  process: ExternalProcess[PIN, OUT],
  timeout: Long,
  capacity: Int)
  extends AbstractStreamOperator[OUT] with OneInputStreamOperator[IN, OUT] {

  require(capacity > 0)

  private val PREPROCESSING_STATE_NAME = "_external_process_operator_preprocessing_state_"
  private val RESULT_STATE_NAME = "_external_process_operator_result_state_"
  private val PROCESS_STATE_NAME = "_external_process_operator_process_state_"

  @transient private var taskLock: AnyRef = _

  @transient private var requestQueue: LinkedBlockingQueue[PendingRequest] = _
  @transient private var processingQueue: LinkedBlockingQueue[PendingRequest] = _
  @transient private var resultLock: Object = _
  @transient private var resultQueue: util.ArrayDeque[PendingResult] = _

  // TODO(JS): Move temporary file handling to MonpolyProcess
  @transient private var tempDirectory: Path = _
  @transient private var tempStateFile: Path = _

  @transient private var snapshotReady: Semaphore = _

  @transient private var preprocessingSerializer: TypeSerializer[preprocessing.State] = _
  @transient private var outSerializer: StreamElementSerializer[OUT] = _
  @transient private var stateSerializer: TypeSerializer[Array[Byte]] = _

  @transient private var recoveredResults: ListState[StreamElement] = _
  @transient private var recoveredPreprocessing: ValueState[preprocessing.State] = _
  @transient private var recoveredState: ValueState[Array[Byte]] = _

  @transient private var pendingCount: Int = 0

  @transient private var waitingRequest: PendingRequest = _

  @transient private var writerThread: ServiceThread = _
  @transient private var readerThread: ServiceThread = _
  @transient private var emitterThread: ServiceThread = _

  @transient private var lastSeenRecord: PIN = _
  @transient @volatile private var printRestoredRecords: Int = _

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
    tempDirectory = Files.createTempDirectory("monitor-process")
    tempDirectory.toFile.deleteOnExit()
    tempStateFile = tempDirectory.resolve("state")
    tempStateFile.toFile.deleteOnExit()
    snapshotReady = new Semaphore(0)
    outSerializer = new StreamElementSerializer[OUT](getOperatorConfig.getTypeSerializerOut(getUserCodeClassloader))

    val preprocessingType = TypeInformation.of(new TypeHint[preprocessing.State]() {})
    preprocessingSerializer = preprocessingType.createSerializer(getExecutionConfig)

    val stateType: TypeInformation[Array[Byte]] = implicitly[TypeInformation[Array[Byte]]]
    stateSerializer = stateType.createSerializer(getExecutionConfig)

    process.setTempFile(tempStateFile)
//    println("DEBUG [setup] complete")
  }

  override def open(): Unit = {
    super.open()

    pendingCount = 0
    waitingRequest = null

    preprocessing.restoreState(None)

    process.start()
//    println("DEBUG [open] process started")

    writerThread = new ServiceThread {
      override def handle(): Unit = {
        val request = requestQueue.take()
        processingQueue.put(request)
//        println("DEBUG [writerThread] request = " + request.getClass)
        request match {
          case InputItem(record) =>
            lastSeenRecord = record.asRecord[PIN]().getValue
            if (printRestoredRecords > 0) {
              println("DEBUG [writerThread] first record after restoring: " + lastSeenRecord.toString)
            }
            printRestoredRecords -= 1
            process.writeRequest(record.asRecord[PIN]().getValue)
          case WatermarkItem(_) => ()
          case SnapshotRequestItem() =>
            if (lastSeenRecord != null) {
              println("DEBUG [writerThread] record immediately before snapshot: " + lastSeenRecord.toString)
            }
            process.initSnapshot()
          case RestoreItem(state) => process.initRestore(state)
          case ShutdownItem() =>
            process.shutdown()
            running = false
        }
      }
    }
    writerThread.start()

    readerThread = new ServiceThread {
      override def handle(): Unit = {
        val request = processingQueue.take()
//        println("DEBUG [readerThread] request = " + request.getClass)

        // FIXME(JS): Do more fine-grained locking; some of the calls below might be blocking!
        resultLock.synchronized {
          request match {
            case InputItem(record) =>
              for (result <- process.readResults())
                resultQueue.add(OutputItem(outputRecord(record.asRecord(), result)))
              resultQueue.add(OutputItem(null))
            case mark@WatermarkItem(_) => resultQueue.add(mark)
            case SnapshotRequestItem() =>
              resultQueue.add(SnapshotResultItem(process.readSnapshot()))
              println("DEBUG [readerThread] snapshot added to resultQueue, releasing semaphore")
              snapshotReady.release()
            case RestoreItem(_) => process.restored()
            case shutdown@ShutdownItem() =>
              process.join()
              resultQueue.add(shutdown)
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

//        println("DEBUG [emitterThread] queue non-empty")

        if (!running)
          return

        taskLock.synchronized {
          var result: PendingResult = null
          resultLock.synchronized {
            result = resultQueue.removeFirst()
          }

//          println("DEBUG [emitterThread] result = " + result.getClass)

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
            case SnapshotResultItem(state) =>
              println("DEBUG [emitterThread] snapshot completed")
            case ShutdownItem() => running = false
          }

          taskLock.notifyAll()
        }
      }
    }
    emitterThread.start()

//    println("DEBUG [open] service threads started")

    printRestoredRecords = 0
    if (recoveredResults != null) {
      printRestoredRecords = 10

      println("DEBUG [ExternalProcessOperator] recovering state")
      assert(Thread.holdsLock(taskLock))
      assert(recoveredPreprocessing != null)
      assert(recoveredState != null)

      setKeyForPartition()

      preprocessing.restoreState(Some(recoveredPreprocessing.value()))

      resultLock.synchronized {
        for (element: StreamElement <- recoveredResults.get) {
          if (element.isRecord)
            resultQueue.add(OutputItem(element.asRecord()))
          else if (element.isWatermark)
            resultQueue.add(WatermarkItem(element.asWatermark()))
          else
            throw new IllegalStateException("Unknown stream element type " + element.getClass +
              " encountered while opening the operator.")
        }

        resultLock.notifyAll()
      }

      requestQueue.put(RestoreItem(recoveredState.value()))

      recoveredPreprocessing = null
      recoveredResults = null
      recoveredState = null
      println("DEBUG [ExternalProcessOperator] state recovered")
    }

    println("DEBUG external process operator is ready")
  }

  override def close(): Unit = {
//    println("DEBUG [close] closing")
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
    println("DEBUG external process operator closed")
  }

  override def dispose(): Unit = {
//    println("DEBUG [dispose] disposing")
    if (writerThread != null)
      writerThread.interruptAndStop()
    if (readerThread != null)
      readerThread.interruptAndStop()
    if (emitterThread != null)
      emitterThread.interruptAndStop()
//    println("DEBUG [dispose] service threads stopped")

    var exception: Exception = null

    try {
      if (process != null)
        process.destroy()
    } catch {
      case e: InterruptedException =>
        exception = e
        Thread.currentThread().interrupt()
      case e: Exception => exception = e
    }

//    println("DEBUG [dispose] process destroyed")
//    if (exception != null) {
//      println("DEBUG [dispose] process.destroy() failed")
//    }

    try {
      if (tempDirectory != null) {
        Files.deleteIfExists(tempStateFile)
        Files.deleteIfExists(tempDirectory)
      }
    } catch {
      case e: InterruptedException =>
        exception = ExceptionUtils.firstOrSuppressed(e, exception)
        Thread.currentThread().interrupt()
      case e: Exception => exception = ExceptionUtils.firstOrSuppressed(e, exception)
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

    println("DEBUG external process operator disposed")
  }

  override def snapshotState(context: StateSnapshotContext): Unit = {
    super.snapshotState(context)
    assert(Thread.holdsLock(taskLock))

    println("DEBUG [snapshotState] snapshot initiated")

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

    println("DEBUG [snapshotState] process state acquired")

    setKeyForPartition()

    // Store the complete state: pending results and the process snapshot.
    val preprocessingState = getKeyedStateStore.getState(new ValueStateDescriptor[preprocessing.State](
      PREPROCESSING_STATE_NAME,
      preprocessingSerializer))
    val resultState = getKeyedStateStore.getListState(new ListStateDescriptor[StreamElement](
      RESULT_STATE_NAME,
      outSerializer))
    val processState = getKeyedStateStore.getState(new ValueStateDescriptor[Array[Byte]](
      PROCESS_STATE_NAME,
      stateSerializer))

    preprocessingState.update(preprocessing.getState)

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

    println("DEBUG [snapshotState] snapshot completed")
  }

  override def initializeState(context: StateInitializationContext): Unit = {
    super.initializeState(context)

    if (context.isRestored) {
      recoveredPreprocessing = getKeyedStateStore.getState(new ValueStateDescriptor[preprocessing.State](
        PREPROCESSING_STATE_NAME,
        preprocessingSerializer))
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

  override def processElement(streamRecord: StreamRecord[IN]): Unit = {
    // TODO(JS): Implement timeout
//    println("DEBUG [processElement]")
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
  def transform[IN, K, PIN, OUT: TypeInformation](
      partitionMapping: Int => Int,
      in: KeyedStream[IN, K],
      preprocessing: Processor[IN, PIN],
      process: ExternalProcess[PIN, OUT],
      capacity: Int): DataStream[OUT] =
    in.transform(
      "External Process",
      new ExternalProcessOperator[IN, PIN, OUT](partitionMapping, preprocessing, process, 0, capacity))
}
