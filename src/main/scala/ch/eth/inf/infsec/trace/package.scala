package ch.eth.inf.infsec

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

package object trace {
  // TODO(JS): Type of data domain?
  // TODO(JS): Consider using a more specialized container type, e.g. Array.
  type Tuple = IndexedSeq[Any]

  case class Event(timestamp: Long, structure: collection.Map[String, Iterable[Tuple]]) {
    override def toString: String = MonpolyFormat.printEvent(this)
  }

  abstract class LineBasedEventParser extends Serializable {
    protected var buffer = new ArrayBuffer[Event]()

    def processLine(line: String)
    def processEnd()

    def bufferedEvents: Seq[Event] = buffer
    def clearBuffer(): Unit = buffer.clear()

    def parseLines(lines: TraversableOnce[String]): Seq[Event] = {
      lines.foreach(processLine)
      processEnd()
      val buffered = buffer
      buffer = new ArrayBuffer[Event]()
      buffered
    }
  }

  trait TraceFormat {
    def createParser(): LineBasedEventParser
  }

  // TODO(JS): Call parser.processEnd() if the stream has ended
  class ParseTraceFunction(format: TraceFormat) extends FlatMapFunction[String, Event] with Serializable {
    private val parser = format.createParser()

    override def flatMap(line: String, collector: Collector[Event]): Unit = {
      parser.processLine(line)
      for (event <- parser.bufferedEvents)
        collector.collect(event)
      parser.clearBuffer()
    }
  }
}
