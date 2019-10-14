package ch.ethz.infsec.tools

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import ch.ethz.infsec.monitor.Fact
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.{KafkaProducer, Partitioner, ProducerRecord}
import org.apache.kafka.common.Cluster

import scala.io.Source
import scala.util.Random
import scala.util.control.Breaks._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

sealed class EndPoint
case class SocketEndpoint(socket_addr: String, port: Int) extends EndPoint
case class FileEndPoint(file_path: String) extends EndPoint
case class KafkaEndpoint() extends EndPoint


class ReorderFactsFunction(numSlices: Int) extends RichFlatMapFunction[Fact, Fact] {
  private class TerminatorMapValue(terminator: Fact) {
    require(terminator.isTerminator)
    private var num: Int = 1

    def incrementNum() : Unit = {
      num += 1
    }

    def getNum: Int = num

    def getTerm: Fact = terminator

  }

  //How often did a terminator appear in the stream
  private val terminatorMap = new mutable.HashMap[Long, TerminatorMapValue]()
  //Map of timestamps to the facts encountered in the stream
  private val factsMap = new mutable.HashMap[Long, ArrayBuffer[Fact]]()
  //The biggest timestamps for which the elements have not already been flushed
  private var currTs: Long = 0

  private def insertElement(value: Fact, timeStamp: Long) : Unit = {
    if (value.isTerminator) {
      //If the fact is a terminator, increment the number for this terminator
      terminatorMap.get(timeStamp) match {
        case Some(term) =>
          if (term.getNum + 1 > numSlices)
            throw new Exception("FATAL ERROR: got more terminators than there are slices")
          term.incrementNum()
        case None => terminatorMap += (timeStamp -> new TerminatorMapValue(value))
      }
    } else {
      //Else append the value to the existing buffer or create a new buffer for this timestamp
      factsMap.get(timeStamp) match {
        case Some(buf) => buf += value
        case None => factsMap += (timeStamp -> new ArrayBuffer[Fact]())
      }
    }
  }

  private def makeBuffer() : Option[mutable.Buffer[Fact]] = {
    val (num, term) = terminatorMap.get(currTs) match {
      case Some(e) => (e.getNum, e.getTerm)
      case None => return None
    }
    if (num == numSlices) {
      var buf = factsMap.getOrElse(currTs, new ArrayBuffer[Fact]())
      buf += term
      terminatorMap -= currTs
      factsMap -= currTs
      currTs += 1
      return Some(buf)
    }
    None
  }

  private def flushReady(out: Collector[Fact]): Unit = {
    breakable {
      while (true) {
        makeBuffer() match {
          case Some(buf) =>
            for (k <- buf)
              out.collect(k)
          case None => break()
        }
      }
    }
  }

  override def flatMap(value: Fact, out: Collector[Fact]): Unit = {
    val timeStamp = value.getTimestamp.toLong
    if (timeStamp < currTs) {
      throw new Exception("FATAL ERROR: Got a timestamp that should already be flushed")
    }
    insertElement(value, timeStamp)

    if (value.isTerminator)
      flushReady(out)
  }
}

class TestSimpleStringSchema extends SimpleStringSchema {
  override def isEndOfStream(nextElement: String): Boolean = nextElement.startsWith("EOF")
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
    props.setProperty("flink.disable-metrics", "false")
    props.setProperty("flink.partition-discovery.interval-millis", "20")
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