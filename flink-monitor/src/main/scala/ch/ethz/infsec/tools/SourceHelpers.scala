package ch.ethz.infsec.tools

import ch.ethz.infsec.kafka.MonitorKafkaConfig
import ch.ethz.infsec.{StreamMonitorBuilder, StreamMonitoring}
import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.slicer.HypercubeSlicer
import ch.ethz.infsec.trace.parser.TraceParser.TerminatorMode
import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SocketTextStreamFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.reflect.io.Path._
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

sealed class MultiSourceVariant {
  def getStreamMonitorBuilder(env: StreamExecutionEnvironment, numSources: Int, slicer: HypercubeSlicer): StreamMonitorBuilder = {
    new StreamMonitorBuilder(env, getReorderFunction(numSources, slicer))
  }

  def getTerminatorMode: TerminatorMode = {
    this match {
      case TotalOrder() => TerminatorMode.ALL_TERMINATORS
      case PerPartitionOrder() => TerminatorMode.ONLY_TIMESTAMPS
      case WaterMarkOrder() => TerminatorMode.NO_TERMINATORS
      case _ => throw new Exception("case failed")
    }
  }

  private def getReorderFunction(numSources: Int, slicer: HypercubeSlicer): ReorderFunction = {
    this match {
      case TotalOrder() => new ReorderTotalOrderFunction(numSources, slicer)
      case PerPartitionOrder() => new ReorderCollapsedPerPartitionFunction(numSources, slicer)
      case WaterMarkOrder() => new ReorderCollapsedWithWatermarksFunction(numSources, slicer)
      case _ => throw new Exception("case failed")
    }
  }
}

case class TotalOrder() extends MultiSourceVariant

case class PerPartitionOrder() extends MultiSourceVariant

case class WaterMarkOrder() extends MultiSourceVariant

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

class KafkaTestProducer(inputDir: String, inputFilePrefix: String) {
  private val topic: String = MonitorKafkaConfig.getTopic
  private val producer: KafkaProducer[String, String] = makeProducer()
  private val inputFiles: Array[(Int, Source)] = inputDir
    .toDirectory
    .files
    .filter(k => k.name matches (inputFilePrefix + ".*\\.csv"))
    .map { k =>
      val PartNumRegex = (inputFilePrefix + """([0-9]+)\.csv""").r
      val PartNumRegex(num) = k.name
      (num.toInt, Source.fromFile(k.path))
    }
    .toArray
  private val numPartitions: Int = MonitorKafkaConfig.getNumPartitions

  require(inputFiles.length == numPartitions, "Kafka must be configured to use the same number of partitions " +
    "as there are input files")
  require(inputFiles.length == inputFiles.map(_._1).distinct.length, "Error with parsing of partition numbers" +
    ", there are duplicates")
  require(inputFiles.forall(k => k._1 >= 0 && k._1 < numPartitions), "Some inputfile numbers are too small/too big")

  //Producer is thread safe according to the kafka documentation
  private class ProducerThread(partNum: Int, src: Source) extends Thread {
    override def run(): Unit = {
      src.getLines().foreach(l => sendRecord(l, partNum))
      sendRecord(">EOF<", partNum)
      sendRecord(">TERMSTREAM<", partNum)
    }
  }

  private def makeProducer(): KafkaProducer[String, String] = new KafkaProducer[String, String](MonitorKafkaConfig.getKafkaProps)

  private def sendRecord(line: String, partition: Int): Unit = {
    producer.send(new ProducerRecord[String, String](topic, partition, "", line + "\n"))
  }

  def runProducer(joinThreads: Boolean = false): Unit = {
    val buf = new ArrayBuffer[ProducerThread]()
    for ((partNum, src) <- inputFiles) {
      val t = new ProducerThread(partNum, src)
      t.start()
      buf += t
    }

    if (joinThreads) {
      buf.foreach(_.join())
    }

    inputFiles.foreach(_._2.close())
  }
}