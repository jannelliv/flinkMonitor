package ch.eth.inf.infsec

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.lang.ProcessBuilder.Redirect
import java.util.Collections
import java.util.concurrent.{ConcurrentLinkedQueue, LinkedBlockingQueue}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{AsyncDataStream => JavaAsyncDataStream}
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConversions
import scala.concurrent.duration.TimeUnit

// TODO(JS): Force monitor to always output something after processing each event
// TODO(JS): Checkpointing
class MonitorFunction(val command: Seq[String]) extends RichAsyncFunction[String, String] {

  @transient private var outputQueue: LinkedBlockingQueue[Option[String]] = _
  @transient private var pendingQueue: ConcurrentLinkedQueue[ResultFuture[String]] = _

  @transient private var process: Process = _
  @transient private var writerThread: Thread = _
  @transient private var readerThread: Thread = _

  // TODO(JS): Logging, error handling, clean-up etc.
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    outputQueue = new LinkedBlockingQueue[Option[String]]()
    pendingQueue = new ConcurrentLinkedQueue[ResultFuture[String]]()

    process = new ProcessBuilder(JavaConversions.seqAsJavaList(command))
      .redirectError(Redirect.INHERIT)
      .start()

    // TODO(JS): Write pid to log
    var pid = -1
    try {
      val f = process.getClass.getDeclaredField("pid")
      f.setAccessible(true)
      pid = f.getInt(process)
      println("MONITOR PID: " + pid)
    } catch {
      case _: Exception =>
    }

    val writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream))
    val reader = new BufferedReader(new InputStreamReader(process.getInputStream))

    writerThread = new Thread {
      //private val logger = Logger.getLogger(this.getClass)
      override def run(): Unit = {
        try {
          var running = true
          while (running) {
            outputQueue.take() match {
              case Some(request: String) =>
                println(s"DEBUG [monitor $pid]: ${request.stripLineEnd}")
                writer.write(request)
                // TODO(JS): Do not flush if there are more requests in the queue
                writer.flush()
              //logger.debug(s"Monitor ${this.hashCode()} - IN: ${record.toString}")
              case None => running = false
            }
          }
        } finally {
          writer.close()
        }
      }
    }
    writerThread.start()

    readerThread = new Thread {
      override def run(): Unit = {
        try {
          var running = true
          do {
            val line = reader.readLine()
            if (line == null)
              running = false
            else {
              // TODO(JS): Check that the line is a proper response
              val resultFuture = pendingQueue.poll()
              resultFuture.complete(Collections.singleton(line))
            }
          } while (running)
        } finally {
          reader.close()
        }
      }
    }
    readerThread.start()
  }

  override def close(): Unit = {
    if (writerThread != null) {
      outputQueue.put(None)
      writerThread.join()
    }
    if (readerThread != null)
      readerThread.join()
    if (process != null)
      process.waitFor()

    super.close()
  }

  override def asyncInvoke(in: String, resultFuture: ResultFuture[String]): Unit = {
    pendingQueue.add(resultFuture)
    outputQueue.put(Some(in))
  }
}

object MonitorFunction {
  def orderedWait(
      input: DataStream[String],
      command: Seq[String],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int): DataStream[String] = {

    val outType: TypeInformation[String] = implicitly[TypeInformation[String]]

    new DataStream[String](JavaAsyncDataStream.orderedWait(
      input.javaStream, new MonitorFunction(command), timeout, timeUnit, capacity
    ).returns(outType))
  }
}
