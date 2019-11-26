package ch.ethz.infsec.tools

import ch.ethz.infsec.kafka.MonitorKafkaConfig
import ch.ethz.infsec.{StreamMonitorBuilder, StreamMonitorBuilderParInput}
import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.trace.parser.TraceParser.TerminatorMode
import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.reflect.io.Path._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object DebugReorderFunction {
  val isDebug: Boolean = true
}

class DebugMap[U] extends MapFunction[U, U] {
  override def map(value: U): U = {
    println("DEBUGMAP: " + System.identityHashCode(this) + " " + value.toString)
    value
  }
}

sealed class MultiSourceVariant {
  override def toString: String = {
    "MULTISOURCE CONFIG\n" +
      "Terminators: " + getTerminatorMode.toString + "\n" +
      "Reorder function: " + getReorderFunction.toString
  }

  def getStreamMonitorBuilder(env: StreamExecutionEnvironment) : StreamMonitorBuilder = {
    new StreamMonitorBuilderParInput(env, getReorderFunction)
  }

  def getTerminatorMode : TerminatorMode = {
    this match {
      case TotalOrder() => TerminatorMode.ALL_TERMINATORS
      case PerPartitionOrder() => TerminatorMode.ONLY_TIMESTAMPS
      case WaterMarkOrder() => TerminatorMode.NO_TERMINATORS
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
  private var currentTs: Long = -1
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
      if (DebugReorderFunction.isDebug)
        println(s"REORDER: got tp $timePoint but currenttp is $currentTp")
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

class KafkaTestProducer(inputDir: String, inputFilePrefix: String) {
  private val topic: String = MonitorKafkaConfig.getTopic
  private val producer: KafkaProducer[String, String] = makeProducer()
  private val inputFiles: Array[(Int, Source)] = inputDir
    .toDirectory
    .files
    .filter(k => k.name matches (inputFilePrefix + ".*\\.csv"))
    .map{k =>
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
    }
  }

  private def makeProducer(): KafkaProducer[String, String] = new KafkaProducer[String, String](MonitorKafkaConfig.getKafkaProps)

  private def sendRecord(line: String, partition: Int) : Unit = {
      producer.send(new ProducerRecord[String, String](topic, partition, "", line + "\n"))
  }

  def runProducer(joinThreads: Boolean = false) : Unit = {
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