package ch.ethz.infsec.monitor

import java.io.FileWriter
import java.util
import java.util.concurrent.locks.{ReentrantLock, Condition}
import java.util.concurrent.{LinkedBlockingQueue, Semaphore}

import ch.ethz.infsec.Processor
import ch.ethz.infsec.slicer.HypercubeSlicer
import ch.ethz.infsec.trace.{CommandRecord, LiftProcessor, MonpolyVerdictFilter, Record}
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
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

private trait PendingRequest

private trait PendingResult

private case class InputItem[IN](input: StreamRecord[IN]) extends PendingRequest

private case class OutputItem[OUT](output: StreamRecord[OUT]) extends PendingResult

private case class OutputSeparatorItem(batchsize:Int) extends PendingResult with PendingRequest

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
  processFactory: ExternalProcessFactory[IN,PIN,POUT,OUT],
  timeout: Long,
  capacity: Int)
  extends AbstractStreamOperator[OUT] with OneInputStreamOperator[IN, OUT] {

  require(capacity > 0)

  private val PREPROCESSING_STATE_NAME = "_external_process_operator_preprocessing_state_"
  private val POSTPROCESSING_STATE_NAME = "_external_process_operator_postprocessing_state_"
  private val RESULT_STATE_NAME = "_external_process_operator_result_state_"
  private val PROCESS_STATE_NAME = "_external_process_operator_process_state_"

  private val preprocessing: Processor[Either[IN,PendingRequest], Either[PIN,PendingRequest]] = processFactory.createPre[PendingRequest,PIN]()
  private val process: ExternalProcess[PIN, POUT] = processFactory.createProc()
  private val postprocessing: Processor[POUT, OUT] = processFactory.createPost()

  @transient private var maxBatchsize:Long = _

  @transient private var taskLock: AnyRef = _

  @transient private var requestQueue: LinkedBlockingQueue[PendingRequest] = _
  @transient private var processingQueue: LinkedBlockingQueue[PendingRequest] = _
  @transient private var resultLock: ReentrantLock = _
  @transient private var resultQueueNonEmpty: Condition = _
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

    maxBatchsize = Math.round(capacity*0.9)
    taskLock = getContainingTask.getCheckpointLock
    requestQueue = new LinkedBlockingQueue[PendingRequest]()
    processingQueue = new LinkedBlockingQueue[PendingRequest]()
    resultLock = new ReentrantLock()
    resultQueueNonEmpty = resultLock.newCondition()
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
    preprocessing.restoreState(preprocessingState.get().asScala.headOption)
    postprocessing.restoreState(postprocessingState.get().asScala.headOption)

    val postProcessState = postprocessingState.get().asScala.headOption
    postprocessing.restoreState(postProcessState)
    postProcessState match {
      case Some(x) =>
        slicer.updateState(x.asInstanceOf[Array[Byte]])
        postprocessing.asInstanceOf[LiftProcessor].accessInternalProcessor.asInstanceOf[MonpolyVerdictFilter].updateProcessingFunction(slicer.mkVerdictFilter)
      case None =>
        postprocessing.asInstanceOf[LiftProcessor].accessInternalProcessor.asInstanceOf[MonpolyVerdictFilter].setCurrent(slicer.getState())
    }
    postprocessing.setParallelInstanceIndex(subtaskIndex)

    resultState.get().asScala.headOption match {
      case Some(results) =>
        for (item <- results)
          resultQueue.add(item)
      case None => ()
    }

    processStates.asScala.headOption match {
      case Some(_) =>
        val relevant = processStates.asScala.filter(_._1 == subtaskIndex)
        if(relevant.size == 1) process.open(relevant.head._2)
        else process.open(relevant)
      case None => process.open()
    }

    preprocessingState.clear()
    postprocessingState.clear()
    processState.clear()
    resultState.clear()

    writerThread = new ServiceThread {
      private var batch = false
      private var batchsize = 0

      override def handle(): Unit = {
        val request = requestQueue.take()

        request match {
          case InputItem(record)   => {
            if (!batch){
              batch=true
              processingQueue.put(request)
            }
            process.writeRequest(record.asRecord[PIN]().getValue)
            batchsize+=1
            if(batchsize>=maxBatchsize){
              process.writeRequest(process.SYNC_BARRIER_IN)
              processingQueue.put(OutputSeparatorItem(batchsize))
              batch=false
              batchsize=0
            }
          }
          case WatermarkItem(_) | LatencyMarkerItem(_) => {
            if (batch) {
              process.writeRequest(process.SYNC_BARRIER_IN)
              processingQueue.put(OutputSeparatorItem(batchsize))
              batch=false
              batchsize=0
            }
            processingQueue.put(request)
          }
          case SnapshotRequestItem() => {
            if (batch) {
              process.writeRequest(process.SYNC_BARRIER_IN)
              processingQueue.put(OutputSeparatorItem(batchsize))
              batch=false
              batchsize=0
            }
            processingQueue.put(request)
            process.initSnapshot()
          }
          case ShutdownItem() => {
            if (batch) {
              process.writeRequest(process.SYNC_BARRIER_IN)
              processingQueue.put(OutputSeparatorItem(batchsize))
              batch=false
              batchsize=0
            }
            processingQueue.put(request)
            process.shutdown()
            running = false
          }
        }
      }
    }
    writerThread.start()

    readerThread = new ServiceThread {
      private var bufferCapacity = 300
      private var transientBuffer = new ArrayBuffer[POUT](1)
      private var buffer = new ArrayBuffer[OutputItem[OUT]](bufferCapacity)


      private def putResult(result: PendingResult): Unit = {
        resultLock.lock()
        try {
          resultQueue.add(result)
          resultQueueNonEmpty.signalAll()
        } finally {
          resultLock.unlock()
        }
      }


      private def processVerdicts():Unit = {
        try {
          for(result <- buffer) {
            resultQueue.add(result)
          }
          resultQueueNonEmpty.signalAll()
        } finally {
          resultLock.unlock()
          buffer.clear()
        }
      }


      override def handle(): Unit = {
        val request = processingQueue.take()

        request match {
          case InputItem(record) => {
            while (true) {
              try {
                process.readResults(transientBuffer)
                assert(transientBuffer.size<=1)

                if (transientBuffer.nonEmpty) {
                  val result = transientBuffer.head
                  if (result != null) {
                    if (!process.SYNC_BARRIER_OUT(result)) {
                      postprocessing.process(
                        result, r => buffer += OutputItem(outputRecord(record, r)))

                      if (buffer.size < bufferCapacity) {
                        if (resultLock.tryLock()) {
                          processVerdicts()
                        }
                      } else {
                        resultLock.lock()
                        processVerdicts()
                      }

                    } else {
                      resultLock.lock()
                      processVerdicts()
                      return
                    }

                  }
                }
              } finally {
                transientBuffer.clear()
              }
            }
          }
          case separator@OutputSeparatorItem(_) => putResult(separator)
          case mark@WatermarkItem(_) => putResult(mark)
          case marker@LatencyMarkerItem(_) => putResult(marker)
          case SnapshotRequestItem() =>
            LoggerFactory.getLogger(getClass).info("SnapshotRequestItem in reader thread")
            val results = process.readSnapshots()
            if(results.size == 1){
              val result = results.head
              putResult(SnapshotResultItem(List((subtaskIndex, result._2))))
            }
            else
              putResult(SnapshotResultItem(results))

            snapshotReady.release()
          case shutdown@ShutdownItem() =>

            process.drainResults(transientBuffer)

            resultLock.lock()
            try{

              for (result <- transientBuffer)
                postprocessing.process(
                  result, r =>
                    resultQueue.add(OutputItem(new StreamRecord[OUT](r))))

              postprocessing.terminate(r =>
                resultQueue.add(OutputItem(new StreamRecord[OUT](r))))
              resultQueueNonEmpty.signalAll()
            } finally {
              resultLock.unlock()
            }
            process.join()
            putResult(shutdown)
            running = false
        }
/*        tempF3.write("end of request\n")
        tempF3.flush()
*/      }
    }
    readerThread.start()

    emitterThread = new ServiceThread {
      override def handle(): Unit = {
        resultLock.lock()
        try{
          while (resultQueue.isEmpty)
            resultQueueNonEmpty.await()
        } finally {
          resultLock.unlock()
        }

        taskLock.synchronized {
          var result: PendingResult = null
          resultLock.lock()
          try{
            if (resultQueue.isEmpty)
              return
            result = resultQueue.removeFirst()
          } finally {
            resultLock.unlock()
          }
          if (result == null)
            return

          // TODO(JS): In AsyncWaitOperator, a TimestampedCollector is used, which recycles a single record object.
          // Do we want to do the same here?
          result match {
            case OutputItem(record) => output.collect(record.asRecord())
            case OutputSeparatorItem(size) => pendingCount -= size
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
      preprocessing.terminate(x => enqueueRequest(wrap(None)(x)))

      requestQueue.put(ShutdownItem())

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
    if (waitingRequest != null) {
      requestQueue.put(waitingRequest)
      pendingCount += 1
      waitingRequest = null
    }
    val logger = LoggerFactory.getLogger(getClass)
    logger.info("inserting snapshotRequestItem")
    requestQueue.put(SnapshotRequestItem())

    logger.info("aquiring snapshotReady")
    // Wait until the process snapshot has been created.
    snapshotReady.acquire()
    logger.info("snapshotReady aquired")
    assert(snapshotReady.availablePermits() == 0)
    assert(requestQueue.isEmpty)

    // Store the complete state: pending results and the process snapshot.

    preprocessingState.clear()
    preprocessingState.add(preprocessing.getState)
    postprocessingState.clear()
    postprocessingState.add(postprocessing.getState)

    resultState.clear()
    processState.clear()
    logger.info("doing snapshot")

    try {
      var gotSnapshot = false
      val results = new ArrayBuffer[PendingResult]()

      resultLock.lock()
      try{
        for (result: PendingResult <- resultQueue.asScala) {
          gotSnapshot = false
          result match {
            case OutputItem(_) => results += result
            case OutputSeparatorItem(_) => results += result
            case WatermarkItem(_) => results += result
            case LatencyMarkerItem(_) => results += result
            case SnapshotResultItem(states) =>
              processState.addAll(states.toList.asJava)
              gotSnapshot = true
            case ShutdownItem() => throw new Exception("Unexpected shutdown of process while taking snapshot.")
          }
        }
      } finally {
        resultLock.unlock()
      }
      assert(gotSnapshot)
      resultState.add(results.toArray)
    } catch {
      case e: Exception =>
        throw new Exception(
          "Could not add pending results to operator state backend of operator" +
            getOperatorName + ".", e)
    }
    logger.info("done snapshot")
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
    preprocessing.process(Right(WatermarkItem(mark)), x =>
      enqueueRequest(wrap(None)(x)))
  }

  override def processLatencyMarker(marker: LatencyMarker): Unit = {
    preprocessing.process(Right(LatencyMarkerItem(marker)), x =>
      enqueueRequest(wrap(None)(x)))
  }

/*  var started = false
  var tempF : FileWriter = null*/


  override def processElement(streamRecord: StreamRecord[IN]): Unit = {
/*    if(!started) {
      started = true
      tempF = new FileWriter("EXPOProcessElement.log",false)
    }
    tempF.write("("+streamRecord.getValue.asInstanceOf[(Int,Record)]._1+","+streamRecord.getValue.asInstanceOf[(Int,Record)]._2.toMonpoly + ")\n")
    tempF.write("PendingCount before this: "+pendingCount+"\n")
    tempF.flush()*/
    // TODO(JS): Implement timeout
    val tuple = streamRecord.getValue.asInstanceOf[(Int, Record)]
    tuple._2 match {
      case CommandRecord("set_slicer", parameters) =>
        postprocessing.asInstanceOf[LiftProcessor].accessInternalProcessor.asInstanceOf[MonpolyVerdictFilter].updatePending(parameters.toCharArray.map(_.toByte))
        pendingSlicer = parameters
      case _ => ()
    }

    // We do not send any commands to unused submonitors. In particular, we cannot use their state fragments
    // because the state is not synchronized with the global progress. Ideally, we would not even create
    // such submonitors.
    // TODO(JS): Check the splitting/merging logic in the case of unused submonitors.
    if (getRuntimeContext.getIndexOfThisSubtask < slicer.degree) {
      preprocessing.process(Left(streamRecord.getValue), x =>
        enqueueRequest(wrap(Some(streamRecord.getTimestamp))(x)))
    }
  }

  private def wrap(timestamp: Option[Long])(in: Either[PIN,PendingRequest]): PendingRequest = {
    in match {
      case Left(p) =>
        timestamp match  {
          case Some(t) => InputItem(new StreamRecord[PIN](p, t))
          case None => InputItem(new StreamRecord[PIN](p))
        }
        case Right(r) => r
      }
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
      processFactory: ExternalProcessFactory[(Int, Record), PIN, POUT, OUT],
      capacity: Int): DataStream[OUT] = {
    in.transform(
      "External Process",
      new ExternalProcessOperator[(Int, Record), PIN, POUT, OUT](slicer, processFactory, 0, capacity))
  }
}
