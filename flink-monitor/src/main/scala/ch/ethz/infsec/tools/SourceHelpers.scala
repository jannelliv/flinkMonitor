package ch.ethz.infsec.tools

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import ch.ethz.infsec.tools.KafkaTestProducer.getTopic
import org.apache.flink.api.common.serialization.SimpleStringSchema
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

class TestSimpleStringSchema extends SimpleStringSchema {
  private val numPartitions = KafkaTestProducer.getNumPartitions(KafkaTestProducer.getTopic)
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
    val numPartitions = cluster.partitionCountForTopic(getTopic)
    if (numPartitions < 2 && !warnedOnce) {
      warnedOnce = true
      println("WARNING: KafkaTestProducer, only 1 possible partition")
    }
    Random.nextInt(numPartitions)
  }

  override def close(): Unit = {}
}

object KafkaTestProducer {
  def getKafkaProps : Properties = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "127.0.0.1:9092")
    props.setProperty("group.id", "lelex")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("partitioner.class", new RandomPartitioner().getClass.getCanonicalName)
    props
  }

  def getTopic: String = "monitor_topic"

  def getNumPartitions(topic: String) : Int = {
    val tmp_producer = new KafkaProducer[Int, String](getKafkaProps)
    val n = tmp_producer.partitionsFor(topic).size()
    if (n < 2) {
      println("WARNING: # partitions is less than 2")
    }
    n
  }

  def sendEOFs(producer: KafkaProducer[Int, String], topic: String) : Unit = {
    producer.flush()
    val numberPartions = getNumPartitions(topic)
    for (i <- 0 until numberPartions) {
      producer.send(new ProducerRecord[Int, String](topic, "EOF#" + i))
    }
    producer.flush()
  }

  def runProducer(csvPath: String, startDelay: Long = 0) : Unit = {
    val admin = AdminClient.create(getKafkaProps)
    val res = admin.deleteTopics(List(getTopic).asJava)
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
        val producer = new KafkaProducer[Int, String](getKafkaProps)
        for (line <- event_lines) {
          val record = new ProducerRecord[Int, String](getTopic, line + "\n")
          producer.send(record)
        }
        sendEOFs(producer, getTopic)
        producer.close(10, TimeUnit.SECONDS)
      }
    }
    thread.start()
  }
}