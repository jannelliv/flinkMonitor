package ch.ethz.infsec.util

import ch.ethz.infsec.kafka.MonitorKafkaConfig
import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.trace.parser.TraceParser.TerminatorMode
import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SocketTextStreamFunction, SourceFunction}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object DebugReorderFunction {
  val isDebug: Boolean = false
}

class DebugMap[U] extends RichMapFunction[U, U] with CheckpointedFunction {
  override def map(value: U): U = {
    println(s"DEBUGMAP for partition ${getRuntimeContext.getIndexOfThisSubtask}, fact: $value")
    value
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    println(s"DEBUGMAP for partition ${getRuntimeContext.getIndexOfThisSubtask}: snapshotting")
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    println(s"DEBUGMAP for partition ${getRuntimeContext.getIndexOfThisSubtask}: initializing")
  }
}

class ParallelSocketTextStreamFunction(hostname: String, port: Int) extends RichParallelSourceFunction[String] {
  var sockSrc: SocketTextStreamFunction = _

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    sockSrc = new SocketTextStreamFunction(hostname, port, "\n", 0)
    sockSrc.run(sourceContext)
  }

  override def cancel(): Unit = sockSrc.cancel()
}


sealed class EndPoint

case class SocketEndpoint(socket_addr: String, port: Int) extends EndPoint

case class FileEndPoint(file_path: String) extends EndPoint

case class KafkaEndpoint() extends EndPoint

@ForwardedFields(Array("1->0; 2->1"))
class SecondThirdMapFunction extends MapFunction[(Int, Int, Fact), (Int, Fact)] {
  override def map(t: (Int, Int, Fact)): (Int, Fact) = {
    (t._2, t._3)
  }
}

@ForwardedFields(Array("0; 1->2"))
class AddSubtaskIndexFunction extends RichFlatMapFunction[(Int, Fact), (Int, Int, Fact)] {
  override def flatMap(value: (Int, Fact), out: Collector[(Int, Int, Fact)]): Unit = {
    val (part, fact) = value
    val idx = getRuntimeContext.getIndexOfThisSubtask
    out.collect((part, idx, fact))
  }
}

class TestSimpleStringSchema extends SimpleStringSchema {
  override def isEndOfStream(nextElement: String): Boolean = nextElement.startsWith(">TERMSTREAM")
}

