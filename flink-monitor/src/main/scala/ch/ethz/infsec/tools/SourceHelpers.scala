package ch.ethz.infsec.tools

import java.util.Properties
import java.util.concurrent.TimeUnit

import ch.ethz.infsec.{StreamMonitorBuilder, StreamMonitorBuilderParInput}
import ch.ethz.infsec.monitor.Fact
import org.apache.commons.math3.distribution.GeometricDistribution
import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
import scala.util.Random
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DebugReorderFunction {
  val isDebug: Boolean = false
}

class DebugMap[U] extends MapFunction[U, U] {
  override def map(value: U): U = {
    println("DEBUGMAP: " + this.hashCode() + " " + value.toString)
    value
  }
}

sealed class MultiSourceVariant {
  override def toString: String = {
    "MULTISOURCE CONFIG\n" +
      "Test producer: " + getTestProducer.toString + "\n" +
      "Terminators: " + !dontSendTerminators() + "\n" +
      "Reorder function: " + getReorderFunction.toString
  }

  def getTestProducer : KafkaTestProducer = {
    this match {
      case TotalOrder() => new PerPartitionOrderProducer()
      case PerPartitionOrder() => new PerPartitionOrderProducer()
      case WaterMarkOrder() => new WaterMarkOrderProducer()
      case _ => throw new Exception("case failed")
    }
  }

  def getStreamMonitorBuilder(env: StreamExecutionEnvironment) : StreamMonitorBuilder = {
    new StreamMonitorBuilderParInput(env, getReorderFunction)
  }

  def dontSendTerminators() : Boolean = {
    this match {
      case TotalOrder() => false
      case PerPartitionOrder() => false
      case WaterMarkOrder() => true
      case _ => throw new Exception("case failed")
    }
  }

  private def getReorderFunction : ReorderFunction = {
    this match {
      case TotalOrder() => ReorderTotalOrderFunction(MonitorKafkaConfig.getNumPartitions)
      case PerPartitionOrder() => ReorderCollapsedPerPartitionFunction(MonitorKafkaConfig.getNumPartitions)
      case WaterMarkOrder() => ReorderCollapsedWithWatermarksFunction(MonitorKafkaConfig.getNumPartitions)
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

sealed abstract class ReorderFunction extends RichFlatMapFunction[(Int, Fact), Fact]

case class ReorderCollapsedPerPartitionFunction(numSources: Int) extends ReorderFunction {
  override def toString: String = "Collapse the facts and order them using terminators"

  private val ts2Facts = new mutable.LongMap[ArrayBuffer[Fact]]()
  private val maxTerminator = (1 to numSources map (_ => -1L)).toArray
  private var currentTs: Long = -1
  private var maxTs: Long = -1
  private var numEOF: Int = 0

  private def insertElement(fact: Fact, idx: Int, timeStamp: Long, out: Collector[Fact]) : Unit = {
    if (fact.isTerminator) {
      if (timeStamp > maxTerminator(idx))
        maxTerminator(idx) = timeStamp
      flushReady(out)
    } else {
      ts2Facts.get(timeStamp) match {
        case Some(buf) => buf += fact
        case None => ts2Facts += (timeStamp -> ArrayBuffer[Fact](fact))
      }
    }
  }

  private def flushReady(out: Collector[Fact]): Unit = {
    val maxAgreedTerminator = maxTerminator.min
    if (maxAgreedTerminator < currentTs)
      return

    for (ts <- (currentTs + 1) to maxAgreedTerminator) {
      ts2Facts
        .get(ts)
        .foreach(_.foreach(out.collect))
      if (DebugReorderFunction.isDebug)
        println(s"REORDER: FLUSHED ${System.identityHashCode(this)}, ts: $ts, currentTS: $currentTs maxTerm: ${maxTerminator.mkString(",")}")
      out.collect(Fact.terminator(ts))
      ts2Facts -= ts
    }
    currentTs = maxAgreedTerminator
  }

  private def forceFlush(out: Collector[Fact]): Unit = {
    if (maxTs != -1) {
      for (ts <- (currentTs + 1) to maxTs) {
        if (DebugReorderFunction.isDebug)
          println(s"REORDER: FORCE FLUSING ${System.identityHashCode(this)}, ts: $ts, currentTS: $currentTs maxTerm: ${maxTerminator.mkString(",")}")
        ts2Facts
          .remove(ts)
          .foreach { facts =>
            facts.foreach(out.collect)
            if (ts > maxTs) {
              maxTs = ts
              out.collect(Fact.terminator(ts))
            }
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

    val timeStamp = fact.getTimestamp
    if(timeStamp > maxTs)
      maxTs = timeStamp

    if (timeStamp < currentTs) {
      if (DebugReorderFunction.isDebug)
        println(s"REORDER: got ts $timeStamp but currentts is $currentTs")
      throw new Exception("FATAL ERROR: Got a timestamp that should already be flushed")
    }
    insertElement(fact, idx, timeStamp, out)
  }
}

case class ReorderCollapsedWithWatermarksFunction(numSources: Int) extends ReorderFunction {
  override def toString: String = "Collapse the facts and reorder them using watermarks"

  private val ts2Facts = new mutable.LongMap[ArrayBuffer[Fact]]()
  private val maxTerminator = (1 to numSources map (_ => -1L)).toArray
  private var currentTs: Long = 0
  private var maxTs: Long = -1
  private var numEOF: Int = 0

  private def insertElement(fact: Fact, idx: Int, timeStamp: Long) : Unit = {
    if (!fact.isTerminator) {
      ts2Facts.get(timeStamp) match {
        case Some(buf) => buf += fact
        case None =>
          ts2Facts += (timeStamp -> ArrayBuffer[Fact](fact))
      }
    }
  }

  private def flushReady(out: Collector[Fact]): Unit = {
    val maxAgreedTerminator = maxTerminator.min
    if (maxAgreedTerminator < currentTs)
      return

    for (ts <- (currentTs + 1) to maxAgreedTerminator) {
      if (DebugReorderFunction.isDebug)
        println(s"REORDER: FLUSING ${System.identityHashCode(this)}, ts: $ts, currentTS: $currentTs maxTerm: ${maxTerminator.mkString(",")}")
      ts2Facts
        .get(ts)
        .foreach(_.foreach(out.collect))

      out.collect(Fact.terminator(ts))
      ts2Facts -= ts
    }
    currentTs = maxAgreedTerminator
  }

  private def forceFlush(out: Collector[Fact]): Unit = {
    if (maxTs != -1) {
      for (ts <- (currentTs + 1) to maxTs) {
        if (DebugReorderFunction.isDebug)
          println(s"REORDER: FORCE FLUSHING ${System.identityHashCode(this)}, ts: $ts, currentTS: $currentTs maxTerm: ${maxTerminator.mkString(",")}")

        ts2Facts
          .remove(ts)
          .foreach { facts =>
            facts.foreach(out.collect)
            if (ts > maxTs) {
              maxTs = ts
              out.collect(Fact.terminator(ts))
            }
          }
      }
    }
  }

  override def flatMap(value: (Int, Fact), out: Collector[Fact]): Unit = {
    val (idx, fact) = value
    if (fact.isMeta) {
      if (fact.getName == "WATERMARK") {
        val timestamp = fact.getArgument(0).asInstanceOf[String].toLong
        if (timestamp > maxTerminator(idx))
          maxTerminator(idx) = timestamp
        flushReady(out)
        return
      } else if (fact.getName == "EOF") {
        numEOF += 1
        if (numEOF == numSources) {
          forceFlush(out)
        }
        return
      }
    }

    val timeStamp = fact.getTimestamp
    if(timeStamp > maxTs)
      maxTs = timeStamp

    if (timeStamp < currentTs) {
      throw new Exception("FATAL ERROR: Got a timestamp that should already be flushed")
    }
    insertElement(fact, idx, timeStamp)
  }
}

case class ReorderTotalOrderFunction(numSources: Int) extends ReorderFunction {
  override def toString: String = "Reorder the facts using timepoints (global order)"

  //Map of timepoints to the facts encountered in the stream
  private val tp2Facts = new mutable.LongMap[ArrayBuffer[Fact]]()
  //Maps timepoints to timestamps
  private val tp2ts = new mutable.LongMap[Long]()
  //The terminator with the highest timepoint per input source
  private val maxTerminator = (1 to numSources map (_ => -1L)).toArray
  private var currentTp: Long = -1
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
          tp2Facts += (timePoint -> ArrayBuffer[Fact](fact))
      }
    }
  }

  private def flushReady(out: Collector[Fact]): Unit = {
    val maxAgreedTerminator = maxTerminator.min
    if (maxAgreedTerminator < currentTp)
      return

    for (tp <- (currentTp + 1) to maxAgreedTerminator) {
      if (DebugReorderFunction.isDebug)
        println(s"REORDER: FLUSING ${System.identityHashCode(this)}, tp: $tp, currentTP: $currentTp maxTerm: ${maxTerminator.mkString(",")}")

      tp2Facts
        .get(tp)
        .foreach(_.foreach(out.collect))

      out.collect(Fact.terminator(tp2ts(tp)))
      tp2Facts -= tp
      tp2ts -= tp
    }
    currentTp = maxAgreedTerminator
  }

  private def forceFlush(out: Collector[Fact]): Unit = {
    if (maxTp != -1) {
      for (tp <- (currentTp + 1) to maxTp) {
        if (DebugReorderFunction.isDebug)
          println(s"REORDER: FORCE FLUSING ${System.identityHashCode(this)}, tp: $tp, currentTP: $currentTp maxTerm: ${maxTerminator.mkString(",")}")
        tp2Facts.remove(tp) match {
          case Some(facts) =>
            facts.foreach(out.collect)
            val timeStamp = tp2ts(tp)
            if (timeStamp > maxTs) {
              maxTs = timeStamp
              out.collect(Fact.terminator(timeStamp))
            }
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
    val timePoint = fact.getTimepoint.longValue()
    val timeStamp = fact.getTimestamp.longValue()
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
  override def isEndOfStream(nextElement: String): Boolean = nextElement.startsWith(">TERMSTREAM")
}

object MonitorKafkaConfig {
  private var initDone: Boolean = false
  private var topicName : String = "monitor_topic"
  private var groupName : String = "monitor"
  private var addr : String = "127.0.0.1:9092"

  def init(
      topicName : String = topicName,
      groupName : String = groupName,
      addr : String = addr
          ) : Unit = {
    MonitorKafkaConfig.topicName = topicName
    MonitorKafkaConfig.groupName = groupName
    MonitorKafkaConfig.addr = addr

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
      throw new Exception("KafkaConfig Object is not initialized, don't forget call init()")
  }

  private def getKafkaPropsInternal: Properties = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", addr)
    props.setProperty("group.id", groupName)
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("flink.disable-metrics", "false")
    props.setProperty("flink.partition-discovery.interval-millis", "20")
    props
  }

  private def getNumPartitionsInternal : Int = {
    val tmp_producer = new KafkaProducer[String, String](MonitorKafkaConfig.getKafkaProps)
    val n = tmp_producer.partitionsFor(topicName).size()
    if (n < 2) {
      throw new Exception("ERROR: # partitions is less than 2")
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

abstract class KafkaTestProducer {
  protected val topic: String = MonitorKafkaConfig.getTopic
  protected val numPartitions: Int = MonitorKafkaConfig.getNumPartitions
  protected val producer: KafkaProducer[String, String] = makeProducer()

  private def makeProducer(): KafkaProducer[String, String] = new KafkaProducer[String, String](MonitorKafkaConfig.getKafkaProps)

  def sendEOFs() : Unit = {
    producer.flush()
    sendRecord(">EOF<", None)
    sendRecord(">TERMSTREAM<", None)
    producer.flush()
  }

  def sendRecord(line: String, partition: Option[Int]) : Unit = {
    partition match {
      case Some(n) =>
        require(n >= 0 && n < numPartitions)
        val record = new ProducerRecord[String, String](MonitorKafkaConfig.getTopic, n, "", line + "\n")
        producer.send(record)
      case None =>
        for (i <- 0 until numPartitions) {
          val topic = MonitorKafkaConfig.getTopic
          val record = new ProducerRecord[String, String](topic, i, "", line + "\n")
          producer.send(record)
        }
    }
  }
  def runProducer(csvPath: String, startDelay: Long = 0) : Unit
}

class WaterMarkOrderProducer extends KafkaTestProducer {
  override def toString: String = "Producing events out of order with watermarks"

  def moveCurrTs(currTs: Long, elementsLeft: Array[Int]) : Int = {
    //Return the index of the first non-zero value of the array
    for (i <- currTs.toInt until elementsLeft.length) {
      if (elementsLeft(i) != 0) {
        return i
      }
    }
    //DONE
    elementsLeft.length
  }

  def sendWatermark(tsVal: Int) : Unit = {
    producer.flush()
    sendRecord(">WATERMARK " + tsVal + "<", None)
    producer.flush()
  }

  def writeToKafka[U <: mutable.Buffer[String]](factMap: mutable.Map[Int, U]) : Unit = {
    val sampler = new GeometricDistribution(0.5)
    val maxTs = factMap.keys.max

    //Lowest TS for which not all facts have been written to kafka
    var currTs = factMap.keys.min

    //TS are the idx of the arr, the values are the numbers of facts left for that TS
    val elementsLeft = new Array[Int](maxTs + 1)

    //Init the number of elements for each key
    factMap.keys.foreach(k => elementsLeft(k) = factMap(k).length)

    while (currTs != maxTs + 1) {
      val currTsRet = moveCurrTs(currTs, elementsLeft)

      //currTS update, send a watermark
      if (currTsRet > currTs) {
        sendWatermark(currTsRet - 1)
      }

      currTs = currTsRet
      //Select a timestamp with a random offset (~ Geo) from the currentTS
      val tsSample = math.min(maxTs, currTs + sampler.sample())
      if (elementsLeft(tsSample) > 0) {
        val buf = factMap(tsSample)
        elementsLeft(tsSample) -= 1
        //Select a random fact in the the set of facts with TS tsSample
        val idxSample = Random.nextInt(buf.length)
        val line = buf.remove(idxSample)
        sendRecord(line, Some(Random.nextInt(numPartitions)))
      }
    }
    sendWatermark(maxTs + 1)
    sendEOFs()
  }

  override def runProducer(csvPath: String, startDelay: Long): Unit = {
    val thread = new Thread {
      override def run(): Unit = {
        if (startDelay != 0)
          Thread.sleep(startDelay)

        val source_file = Source.fromFile(csvPath)
        val source_as_str = source_file.mkString
        source_file.close()
        //Split input into lines
        val event_lines = source_as_str.split('\n')
        //Extract the timestamps and group by timestamps
        val tsToLines = event_lines
          .map(k => {
            val ts = k.split(',')(2).split('=')(1).toInt
            (ts, k)
          })
          .groupBy(_._1)
          .mapValues(k => mutable.ArrayBuffer(k.map(_._2) :_* ))
        //Randomize the trace and write it to kafka
        writeToKafka(mutable.Map() ++= tsToLines)
        producer.close(10, TimeUnit.SECONDS)
      }
    }
    thread.start()
    thread.join()
  }
}

class PerPartitionOrderProducer extends KafkaTestProducer {
  override def toString: String = "Producing events in order per partition"

  def runProducer(csvPath: String, startDelay: Long = 0) : Unit = {
    val thread = new Thread {
      override def run(): Unit = {
        if (startDelay != 0)
          Thread.sleep(startDelay)
        val source_file = Source.fromFile(csvPath)
        val source_as_str = source_file.mkString
        source_file.close()
        val event_lines = source_as_str.split('\n')
        for (line <- event_lines) {
          sendRecord(line, Some(Random.nextInt(numPartitions)))
        }
        sendEOFs()
        producer.close(10, TimeUnit.SECONDS)
      }
    }
    thread.start()
    thread.join()
  }
}