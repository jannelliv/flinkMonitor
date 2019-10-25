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
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

sealed class EndPoint
case class SocketEndpoint(socket_addr: String, port: Int) extends EndPoint
case class FileEndPoint(file_path: String) extends EndPoint
case class KafkaEndpoint() extends EndPoint

class AddSubtaskIndexFunction extends RichFlatMapFunction[(Int, Fact), (Int, Int, Fact)] {
  var cached_idx : Option[Int] = None
  override def flatMap(value: (Int, Fact), out: Collector[(Int, Int, Fact)]): Unit = {
    val (part, fact) = value
    val idx = cached_idx.getOrElse(getRuntimeContext.getIndexOfThisSubtask)
    if (cached_idx.isEmpty)
      cached_idx = Some(idx)
    out.collect((part, idx, fact))
  }
}

class ReorderFactsFunction(numSources: Int) extends RichFlatMapFunction[(Int, Fact), Fact] {
  //Map of timepoints to the facts encountered in the stream
  private val tp2Facts = new mutable.LongMap[ArrayBuffer[Fact]]()
  //Maps timepoints to timestamps
  private val tp2ts = new mutable.LongMap[Long]()
  //The terminator with the highest timepoint per input source
  private val maxTerminator = (1 to numSources map (_ => -1L)).toArray
  //The biggest (TP + 1) for which the elements have already been flushed
  private var currentTp: Long = 0
  //The biggest Tp that was ever received
  private var maxTp: Long = -1
  //The biggest Ts that was ever flushed
  private var maxTs: Long = -1
  //Number of EOFs that have been received
  private var numEOF: Int = 0

  private def insertElement(fact: Fact, idx: Int, timePoint: Long) : Unit = {
    if (fact.isTerminator) {
      //If the terminator has a higher tp than the current tp for this partition, update the tp
      if (timePoint > maxTerminator(idx))
        maxTerminator(idx) = timePoint
    } else {
      //Else, append the value to the existing buffer (or create a new buffer for this timepoint)
      tp2Facts.get(timePoint) match {
        case Some(buf) => buf += fact
        case None =>
          val buf = new ArrayBuffer[Fact]()
          buf += fact
          tp2Facts += (timePoint -> buf)
      }
    }
  }

  private def flushReady(out: Collector[Fact]): Unit = {
    val maxAgreedTerminator = maxTerminator.min
    if (maxAgreedTerminator < currentTp)
      return

    println("UPDATING IS: " + maxTerminator.mkString(", "))

    for (tp <- currentTp to maxAgreedTerminator) {
      val buf = tp2Facts.get(tp)

      buf match {
        case Some(buf) =>
          for (k <- buf)
            out.collect(k)
        case None => ()
      }

      out.collect(Fact.terminator(tp2ts(tp).toString))
      //println("Flushing TP: " + this.hashCode() + " " + tp)
      tp2Facts -= tp
      tp2ts -= tp
    }
    currentTp = maxAgreedTerminator + 1
  }

  private def forceFlush(out: Collector[Fact]): Unit = {
    //println("FORCE FLUSH TERM MAP DUMP FOR " + this.hashCode() + " " + mapDump())
    if (maxTp != -1) {
      for (tp <- currentTp to maxTp) {
        tp2Facts.remove(tp) match {
          case Some(facts) =>
            for (k <- facts)
              out.collect(k)
            val timeStamp = tp2ts(tp)
            if (timeStamp > maxTs) {
              maxTs = timeStamp
              out.collect(Fact.terminator(timeStamp.toString))
            }
            //println("FORCE FLUSH FOR TP: " + this.hashCode() + " " + tp)
          case None => ()
        }
      }
    }
  }

  override def flatMap(value: (Int, Fact), out: Collector[Fact]): Unit = {
    val (idx, fact) = value
    if (fact.isMeta && fact.getName == "EOF") {
      numEOF += 1
      if (numEOF == numSources) {
        forceFlush(out)
      }
      return
    }
    //println("FLUSH GOT TIMEPOINT: " + this.hashCode() + " " + value.getTimepoint)
    val timePoint = fact.getTimepoint.toLong
    val timeStamp = fact.getTimestamp.toLong
    tp2ts += (timePoint -> timeStamp)
    if(timePoint > maxTp)
      maxTp = timePoint

    if (timePoint < currentTp) {
      throw new Exception("FATAL ERROR: Got a timepoint that should already be flushed")
    }
    insertElement(fact, idx, timePoint)

    if (fact.isTerminator)
      flushReady(out)
  }
}

class TestSimpleStringSchema extends SimpleStringSchema {
  override def isEndOfStream(nextElement: String): Boolean = nextElement.startsWith("TERMSTREAM")
}

class RandomPartitioner extends Partitioner {
  private var warnedOnce: Boolean = false

  override def configure(map: util.Map[String, _]): Unit = {}

  override def partition(s: String, o: Any, bytes: Array[Byte], o1: Any, bytes1: Array[Byte], cluster: Cluster): Int = {
    val line = o1.asInstanceOf[String]
    val line_parts = line.split('#')
    if (line_parts.length == 2 && (line_parts(0).equalsIgnoreCase("EOF") || line_parts(0).equalsIgnoreCase("TERMSTREAM"))) {
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
  private var initDone: Boolean = false
  private var topicName : String = "monitor_topic"
  private var groupName : String = "monitor"
  private var addr : String = "127.0.0.1:9092"
  private var partitioner : Partitioner = new RandomPartitioner

  def init(
      topicName : String = topicName,
      groupName : String = groupName,
      addr : String = addr,
      partitioner : Partitioner = partitioner
          ) : Unit = {
    MonitorKafkaConfig.topicName = topicName
    MonitorKafkaConfig.groupName = groupName
    MonitorKafkaConfig.addr = addr
    MonitorKafkaConfig.partitioner = partitioner

    val admin = AdminClient.create(MonitorKafkaConfig.getKafkaPropsInternal)
    val res = admin.deleteTopics(List(MonitorKafkaConfig.getTopicInternal).asJava)
    try {
      res.all().get(10, TimeUnit.SECONDS)
    } catch {
      case _: Throwable => println("Kafka topic does not exist, creating it")
    }
    admin.close(10, TimeUnit.SECONDS)
    initDone = true
  }

  private def checkInit(): Unit = {
    if (!initDone)
      throw new Exception("KafkaConfig Object is not initialized")
  }

  private def getKafkaPropsInternal: Properties = {
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

  private def getNumPartitionsInternal : Int = {
    val tmp_producer = new KafkaProducer[Int, String](MonitorKafkaConfig.getKafkaProps)
    val n = tmp_producer.partitionsFor(topicName).size()
    if (n < 2) {
      println("WARNING: # partitions is less than 2")
    }
    n
  }

  private def getTopicInternal: String = topicName

  def getKafkaProps : Properties = {
    checkInit()
    getKafkaPropsInternal
  }

  def getNumPartitions : Int = {
    checkInit()
    getNumPartitionsInternal
  }

  def getTopic: String = {
    checkInit()
    getTopicInternal
  }
}

object KafkaTestProducer {
  def sendEOFs(producer: KafkaProducer[Int, String], topic: String) : Unit = {
    producer.flush()
    val numPartitions = MonitorKafkaConfig.getNumPartitions
    for (i <- 0 until numPartitions) {
      producer.send(new ProducerRecord[Int, String](topic, "EOF#" + i))
      producer.flush()
      producer.send(new ProducerRecord[Int, String](topic, "TERMSTREAM#" + i))
      producer.flush()
    }
  }

  def runProducer(csvPath: String, startDelay: Long = 0) : Unit = {
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