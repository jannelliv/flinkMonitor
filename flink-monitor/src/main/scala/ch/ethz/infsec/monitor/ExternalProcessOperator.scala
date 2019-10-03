package ch.ethz.infsec.monitor

import java.util
import java.util.concurrent.locks.{Condition, ReentrantLock}
import java.util.concurrent.{LinkedBlockingQueue, Semaphore}

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.runtime.state.{StateInitializationContext, StateSnapshotContext}
import org.apache.flink.streaming.api.graph.StreamConfig
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, ChainingStrategy, OneInputStreamOperator, Output}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.{LatencyMarker, StreamRecord}
import org.apache.flink.streaming.runtime.tasks.StreamTask
import org.apache.flink.util.ExceptionUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class ExternalProcessOperator[IN, OUT](process: ExternalProcess[IN, OUT], capacity: Int)
  extends AbstractStreamOperator[OUT] with OneInputStreamOperator[IN, OUT] {

  chainingStrategy = ChainingStrategy.ALWAYS

  require(capacity > 0)
  private val maxBatchSize: Int = capacity / 10

  private val MARKER_STATE_NAME = "_external_process_operator_marker_state_"
  private val OUTPUT_STATE_NAME = "_external_process_operator_output_state_"
  private val PROCESS_STATE_NAME = "_external_process_operator_process_state_"

  private trait InputItem

  private trait SyncItem

  private trait OutputItem

  private case class DataInputItem(data: IN) extends InputItem

  private case class DataOutputItem(data: OUT) extends OutputItem

  private case class WatermarkItem(mark: Watermark) extends InputItem with SyncItem with OutputItem

  private case class LatencyMarkerItem(mark: LatencyMarker) extends InputItem with SyncItem with OutputItem

  private case class SnapshotRequestItem() extends InputItem with SyncItem

  private case class ShutdownItem() extends InputItem with SyncItem with OutputItem

  private case class WakeupItem() extends SyncItem with OutputItem

  @transient private var taskLock: AnyRef = _

  @transient private var acceptingMarkers: Boolean = _
  @transient private var markerQueue: ArrayBuffer[InputItem] = _
  @transient private var pendingInput: InputItem = _
  @transient private var inputQueue: LinkedBlockingQueue[InputItem] = _
  @transient private var syncQueue: LinkedBlockingQueue[SyncItem] = _
  @transient private var outputLock: ReentrantLock = _
  @transient private var outputQueueNonEmpty: Condition = _
  @transient private var outputQueue: util.ArrayDeque[OutputItem] = _

  @transient private var processSnapshotReady: Semaphore = _
  @transient private var processSnapshot: Seq[Array[Byte]] = _

  @transient private var markerSerializer: TypeSerializer[Array[InputItem]] = _
  @transient private var outputSerializer: TypeSerializer[Array[OutputItem]] = _
  @transient private var stateSerializer: TypeSerializer[(Int, Array[Byte])] = _

  @transient private var markerState: ListState[Array[InputItem]] = _
  @transient private var outputState: ListState[Array[OutputItem]] = _
  @transient private var processState: ListState[(Int, Array[Byte])] = _

  @transient private var writerThread: ServiceThread = _
  @transient private var readerThread: ServiceThread = _
  @transient private var emitterThread: ServiceThread = _

  override def setup(
                      containingTask: StreamTask[_, _],
                      config: StreamConfig,
                      output: Output[StreamRecord[OUT]]): Unit = {

    super.setup(containingTask, config, output)

    taskLock = getContainingTask.getCheckpointLock
    markerQueue = new ArrayBuffer[InputItem]()
    inputQueue = new LinkedBlockingQueue[InputItem]()
    syncQueue = new LinkedBlockingQueue[SyncItem]()
    outputLock = new ReentrantLock()
    outputQueueNonEmpty = outputLock.newCondition()
    outputQueue = new util.ArrayDeque[OutputItem]()

    processSnapshotReady = new Semaphore(0)

    val inputListType = TypeInformation.of(new TypeHint[Array[InputItem]] {})
    markerSerializer = inputListType.createSerializer(getExecutionConfig)

    val outputListType = TypeInformation.of(new TypeHint[Array[OutputItem]]() {})
    outputSerializer = outputListType.createSerializer(getExecutionConfig)

    val stateType = TypeInformation.of(new TypeHint[(Int, Array[Byte])]() {})
    stateSerializer = stateType.createSerializer(getExecutionConfig)

    // FIXME(JS): Figure out how to do this so that abstraction boundaries are respected.
    /*
    // TODO(JS): This is specific to the monitoring application.
    getMetricGroup.gauge[Long, Gauge[Long]]("numEvents", new Gauge[Long] {
      override def getValue: Long = preprocessing.getCustomCounter
    })
    */
  }

  override def open(): Unit = {
    super.open()
    assert(Thread.holdsLock(taskLock))
    val logger = LoggerFactory.getLogger(getClass)

    acceptingMarkers = true
    pendingInput = null

    val subtaskIndex = getRuntimeContext.getIndexOfThisSubtask
    process.identifier = Some(subtaskIndex.toString)

    markerState.get().asScala.headOption match {
      case Some(items) =>
        logger.info("restoring {} marker items", items.length)
        for (item <- items)
          inputQueue.add(item)
      case None => ()
    }

    outputState.get().asScala.headOption match {
      case Some(items) =>
        logger.info("restoring {} output items", items.length)
        for (item <- items)
          outputQueue.add(item)
      case None => ()
    }

    val processStates = processState.get().asScala.filter(_._1 == subtaskIndex).map(_._2)
    processStates.size match {
      case 0 => process.open()
      case 1 =>
        logger.info("restoring single process state")
        process.openWithState(processStates.head)
      case _ =>
        logger.info("merging process states")
        process.openAndMerge(processStates)
    }

    markerState.clear()
    outputState.clear()
    processState.clear()

    writerThread = new ServiceThread {
      private var batchSize = 0

      override def handle(): Unit = {
        val request = inputQueue.take()
        request match {
          case DataInputItem(data) =>
            process.writeItem(data)
            batchSize += 1
            if (batchSize >= maxBatchSize) {
              syncQueue.put(WakeupItem())
              process.writeSyncBarrier()
              batchSize = 0
            }
          case item@WatermarkItem(_) =>
            syncQueue.put(item)
            process.writeSyncBarrier()
            batchSize = 0
          case item@LatencyMarkerItem(_) =>
            syncQueue.put(item)
            process.writeSyncBarrier()
            batchSize = 0
          case item@SnapshotRequestItem() =>
            syncQueue.put(item)
            process.writeSyncBarrier()
            process.initSnapshot()
            batchSize = 0
          case item@ShutdownItem() =>
            syncQueue.put(item)
            process.writeSyncBarrier()
            process.shutdown()
            batchSize = 0
            running = false
        }
      }
    }
    writerThread.start()

    readerThread = new ServiceThread {
      private def putOutput(item: OutputItem): Unit = {
        outputLock.lock()
        try {
          outputQueue.add(item)
          outputQueueNonEmpty.signalAll()
        } finally {
          outputLock.unlock()
        }
      }

      override def handle(): Unit = {
        process.readResults(x => putOutput(DataOutputItem(x)))
        val request = syncQueue.take()
        request match {
          case item@WatermarkItem(_) => putOutput(item)
          case item@LatencyMarkerItem(_) => putOutput(item)
          case item@WakeupItem() => putOutput(item)
          case SnapshotRequestItem() =>
            processSnapshot = process.readSnapshot()
            processSnapshotReady.release()
          case item@ShutdownItem() =>
            process.readResults(x => putOutput(DataOutputItem(x)))
            // TODO(JS): Log non-zero exit code.
            process.join()
            putOutput(item)
            running = false
        }
      }
    }
    readerThread.start()

    emitterThread = new ServiceThread {
      override def handle(): Unit = {
        outputLock.lock()
        try {
          while (outputQueue.isEmpty)
            outputQueueNonEmpty.await()
        } finally {
          outputLock.unlock()
        }

        taskLock.synchronized {
          var request: OutputItem = null
          outputLock.lock()
          try {
            if (outputQueue.isEmpty)
              return
            request = outputQueue.removeFirst()
          } finally {
            outputLock.unlock()
          }
          if (request == null)
            return

          // TODO(JS): In AsyncWaitOperator, a TimestampedCollector is used, which recycles a single record object.
          // Do we want to do the same here?
          request match {
            case DataOutputItem(data) => output.collect(new StreamRecord[OUT](data))
            case WatermarkItem(mark) => output.emitWatermark(mark)
            case WakeupItem() => ()
            case LatencyMarkerItem(marker) => output.emitLatencyMarker(marker)
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
      for (marker <- markerQueue)
        inputQueue.put(marker)
      markerQueue.clear()
      inputQueue.put(ShutdownItem())

      if (Thread.holdsLock(taskLock)) {
        while (emitterThread.isAlive)
          taskLock.wait(100L)
      }

      writerThread.join()
      readerThread.join()
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
    if (pendingInput != null) {
      inputQueue.put(pendingInput)
      pendingInput = null
    }
    val logger = LoggerFactory.getLogger(getClass)
    logger.info("inserting snapshotRequestItem")
    inputQueue.put(SnapshotRequestItem())

    logger.info("aquiring snapshotReady")
    // Wait until the process snapshot has been created.
    processSnapshotReady.acquire()
    logger.info("snapshotReady aquired")
    assert(processSnapshotReady.availablePermits() == 0)
    assert(inputQueue.isEmpty)

    // Store the complete state: pending results and the process snapshot.

    markerState.clear()
    outputState.clear()
    processState.clear()
    logger.info("storing snapshot")

    markerState.add(markerQueue.toArray)

    outputLock.lock()
    try {
      outputState.add(outputQueue.toArray(new Array[OutputItem](0)))
    } finally {
      outputLock.unlock()
    }

    val subtaskIndex = getRuntimeContext.getIndexOfThisSubtask
    if (processSnapshot.length == 1) {
      processState.add((subtaskIndex, processSnapshot.head))
    } else {
      processState.addAll(processSnapshot.zipWithIndex.map(_.swap).asJava)
    }
    processSnapshot = null

    logger.info("snapshot completed")
  }

  override def initializeState(context: StateInitializationContext): Unit = {
    super.initializeState(context)

    markerState = getOperatorStateBackend.getListState(new ListStateDescriptor[Array[InputItem]](
      MARKER_STATE_NAME, markerSerializer))
    outputState = getOperatorStateBackend.getUnionListState(new ListStateDescriptor[Array[OutputItem]](
      OUTPUT_STATE_NAME,
      outputSerializer))
    processState = getOperatorStateBackend.getUnionListState(new ListStateDescriptor[(Int, Array[Byte])](
      PROCESS_STATE_NAME,
      stateSerializer))
  }

  private def enqueueInput(item: InputItem): Unit = {
    assert(Thread.holdsLock(taskLock))
    pendingInput = item
    while (pendingInput != null && inputQueue.size() >= capacity)
      taskLock.wait()
    if (pendingInput != null) {
      inputQueue.put(pendingInput)
      pendingInput = null
    }
  }

  private def enqueueMarker(marker: InputItem): Unit = {
    if (acceptingMarkers)
      enqueueInput(marker)
    else
      markerQueue += marker
  }

  override def processWatermark(mark: Watermark): Unit = enqueueMarker(WatermarkItem(mark))

  override def processLatencyMarker(marker: LatencyMarker): Unit = enqueueMarker(LatencyMarkerItem(marker))

  override def processElement(streamRecord: StreamRecord[IN]): Unit = {
    val value = streamRecord.getValue
    enqueueInput(DataInputItem(value))
    acceptingMarkers = process.enablesSyncBarrier(value)
    if (acceptingMarkers) {
      for (marker <- markerQueue)
        enqueueInput(marker)
      markerQueue.clear()
    }
  }

  def failOperator(throwable: Throwable): Unit = {
    getContainingTask.getEnvironment.failExternally(throwable)
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
  // TODO(JS): Do we need to "clean" the process?
  def transform[IN, OUT: TypeInformation](in: DataStream[IN], process: ExternalProcess[IN, OUT], capacity: Int): DataStream[OUT] = {
    in.transform(
      "External Process",
      new ExternalProcessOperator[IN, OUT](process, capacity))
  }
}
