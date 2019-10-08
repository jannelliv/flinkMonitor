package ch.ethz.infsec.tools

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import ch.ethz.infsec.monitor.Fact
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.{KafkaProducer, Partitioner, ProducerRecord}
import org.apache.kafka.common.Cluster

import scala.io.Source
import scala.util.Random
import scala.collection.JavaConverters._

sealed class EndPoint
case class SocketEndpoint(socket_addr: String, port: Int) extends EndPoint
case class FileEndPoint(file_path: String) extends EndPoint
case class KafkaEndpoint() extends EndPoint

class ReorderFactsFunction extends AllWindowFunction[(Int, Fact), (Int, Fact), TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[(Int, Fact)], out: Collector[(Int, Fact)]): Unit = {
    println("REORDER: Asked to reorder " + input.size + "elements")
    val inSorted = input
      .toArray
      .sortWith{ case ((_, fst), (_, snd)) => fst.getTimestamp.toLong < snd.getTimestamp.toLong }
    for (k <- inSorted) {
      out.collect(k)
    }
  }
}

class TestSimpleStringSchema extends SimpleStringSchema {
  private val numPartitions = MonitorKafkaConfig.getNumPartitions
  private val gotEof: Array[Boolean] = new Array[Boolean](numPartitions)

  override def isEndOfStream(nextElement: String): Boolean = {
    val elem_parts = nextElement.split('#')
    if (elem_parts.length == 2 && elem_parts(0).equalsIgnoreCase("EOF")) {
      gotEof(elem_parts(1).toInt) = true
      println("Got EOF for partition # " + elem_parts(1).toInt)
      if (gotEof.forall{_ == true}) {
        println("Terminating now, got all EOFs")
        return true
      }
    }
    false
  }
}

class RandomPartitioner extends Partitioner {
  private var warnedOnce: Boolean = false

  override def configure(map: util.Map[String, _]): Unit = {}

  override def partition(s: String, o: Any, bytes: Array[Byte], o1: Any, bytes1: Array[Byte], cluster: Cluster): Int = {
    val line = o1.asInstanceOf[String]
    val line_parts = line.split('#')
    if (line_parts.length == 2 && line_parts(0).equalsIgnoreCase("EOF")) {
      return line_parts(1).toInt
    }
    val numPartitions = cluster.partitionCountForTopic(MonitorKafkaConfig.getTopic)
    if (numPartitions < 2 && !warnedOnce) {
      warnedOnce = true
      println("WARNING: KafkaTestProducer, only 1 possible partition")
    }
    Random.nextInt(numPartitions)
  }

  override def close(): Unit = {}
}

object MonitorKafkaConfig {
  private var topicName : String = "monitor_topic"
  private var groupName : String = "monitor"
  private var numPartitions : Int = 8
  private var addr : String = "127.0.0.1:9092"
  private var partitioner : Partitioner = new RandomPartitioner

  def getNumPartitions : Int = getNumPartitionsChecked
  def getTopic: String = topicName

  def init(
      topicName : String = topicName,
      groupName : String = groupName,
      numPartitions : Int = numPartitions,
      addr : String = addr,
      partitioner : Partitioner = partitioner
          ) : Unit = {
    if (numPartitions != MonitorKafkaConfig.numPartitions) {
      MonitorKafkaConfig.numPartitions = numPartitions
      updateNumPartitions()
    }
    MonitorKafkaConfig.topicName = topicName
    MonitorKafkaConfig.groupName = groupName
    MonitorKafkaConfig.addr = addr
    MonitorKafkaConfig.partitioner = partitioner
  }

  def getKafkaProps : Properties = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", addr)
    props.setProperty("group.id", groupName)
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("partitioner.class", partitioner.getClass.getCanonicalName)
    props
  }

  private def updateNumPartitions() : Unit = {

  }

  private def getNumPartitionsChecked : Int = {
    val tmp_producer = new KafkaProducer[Int, String](MonitorKafkaConfig.getKafkaProps)
    val n = tmp_producer.partitionsFor(topicName).size()
    if (n < 2) {
      println("WARNING: # partitions is less than 2")
    }
    n
  }
}

object KafkaTestProducer {
  def sendEOFs(producer: KafkaProducer[Int, String], topic: String) : Unit = {
    producer.flush()
    val numPartitions = MonitorKafkaConfig.getNumPartitions
    for (i <- 0 until numPartitions) {
      producer.send(new ProducerRecord[Int, String](topic, "EOF#" + i))
    }
    producer.flush()
  }

  def runProducer(csvPath: String, startDelay: Long = 0) : Unit = {
    val admin = AdminClient.create(MonitorKafkaConfig.getKafkaProps)
    val res = admin.deleteTopics(List(MonitorKafkaConfig.getTopic).asJava)
    try {
      res.all().get(10, TimeUnit.SECONDS)
    } catch {
      case _: Throwable => println("Kafka topic does not exist, creating it")
    }
    admin.close(10, TimeUnit.SECONDS)
    val thread = new Thread {
      override def run(): Unit = {
        if (startDelay != 0) {
          Thread.sleep(startDelay)
        }
        val source_file = Source.fromFile(csvPath)
        val source_as_str = source_file.mkString
        source_file.close()
        val event_lines = source_as_str.split('\n')
        val producer = new KafkaProducer[Int, String](MonitorKafkaConfig.getKafkaProps)
        for (line <- event_lines) {
          val record = new ProducerRecord[Int, String](MonitorKafkaConfig.getTopic, line + "\n")
          producer.send(record)
        }
        sendEOFs(producer, MonitorKafkaConfig.getTopic)
        producer.close(10, TimeUnit.SECONDS)
      }
    }
    thread.start()
  }
}