package ch.ethz.infsec.tools

import ch.ethz.infsec.kafka.MonitorKafkaConfig
import ch.ethz.infsec.{StreamMonitorBuilder, StreamMonitorBuilderParInput}
import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.trace.parser.TraceParser.TerminatorMode
import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.reflect.io.Path._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object DebugReorderFunction {
  val isDebug: Boolean = false
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

  def getStreamMonitorBuilder(env: StreamExecutionEnvironment): StreamMonitorBuilder = {
    new StreamMonitorBuilderParInput(env, getReorderFunction)
  }

  def getTerminatorMode: TerminatorMode = {
    this match {
      case TotalOrder() => TerminatorMode.ALL_TERMINATORS
      case PerPartitionOrder() => TerminatorMode.ONLY_TIMESTAMPS
      case WaterMarkOrder() => TerminatorMode.NO_TERMINATORS
      case _ => throw new Exception("case failed")
    }
  }

  private def getReorderFunction: ReorderFunction = {
    this match {
      case TotalOrder() => new ReorderTotalOrderFunction()
      case PerPartitionOrder() => new ReorderCollapsedPerPartitionFunction()
      case WaterMarkOrder() => new ReorderCollapsedWithWatermarksFunction()
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

/*
abstract class ReorderFunction extends RichFlatMapFunction[(Int, Fact), Fact] with CheckpointedFunction {
  protected val numSources: Int = MonitorKafkaConfig.getNumPartitions
  private val idx2Facts = new mutable.LongMap[ArrayBuffer[Fact]]()
  private var maxOrderElem = (1 to numSources map (_ => -1L)).toArray
  private var currentIdx: Long = -2
  private var maxIdx: Long = -1
  private var numEOF: Int = 0

  @transient private var idx2facts_state : ListState[(Long, ArrayBuffer[Fact])] = _
  @transient private var maxorderelem_state : ListState[Long] = _
  @transient private var indices_state : ListState[Long] = _

  protected def isOrderElement(fact: Fact): Boolean

  protected def indexExtractor(fact: Fact): Long

  protected def makeTerminator(idx: Long): Fact

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    idx2facts_state.clear()
    maxorderelem_state.clear()
    indices_state.clear()
    idx2Facts.foreach(k => idx2facts_state.add(k))
    maxOrderElem.foreach(k => maxorderelem_state.add(k))
    indices_state.add(currentIdx)
    indices_state.add(maxIdx)
    indices_state.add(numEOF)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val desc_idx2facts = new ListStateDescriptor[(Long, ArrayBuffer[Fact])](
      "idx2facts map",
      TypeInformation.of(new TypeHint[(Long, ArrayBuffer[Fact])] {})
    )
    val desc_maxorderelem = new ListStateDescriptor[Long](
      "maxorderelem",
      TypeInformation.of(new TypeHint[Long] {})
    )
    val desc_indices = new ListStateDescriptor[Long](
      "desc_indices",
      TypeInformation.of(new TypeHint[Long] {})
    )
    idx2facts_state = context.getOperatorStateStore.getListState(desc_idx2facts)
    maxorderelem_state = context.getOperatorStateStore.getListState(desc_maxorderelem)
    indices_state = context.getOperatorStateStore.getListState(desc_indices)

    if (context.isRestored) {
      idx2facts_state.get().forEach(k => idx2Facts += (k._1, k._2))
      maxOrderElem = maxorderelem_state.get().asScala.toArray
      val indices = indices_state.get().asScala.toArray
      require(indices.length == 3)
      currentIdx = indices(0)
      maxIdx = indices(1)
      numEOF = indices(2).toInt
    }
  }

  private def insertElement(fact: Fact, idx: Int, timeStamp: Long, out: Collector[Fact]): Unit = {
    idx2Facts.get(timeStamp) match {
      case Some(buf) => buf += fact
      case None => idx2Facts += (timeStamp -> ArrayBuffer[Fact](fact))
    }
  }

  private def flushReady(out: Collector[Fact]): Unit = {
    val maxAgreedIdx = maxOrderElem.min
    if (maxAgreedIdx < currentIdx)
      return

    var idx = currentIdx + 1
    while (idx <= maxAgreedIdx) {
      idx2Facts
        .remove(idx)
        .foreach{ k =>
          var i = 0
          val len = k.length
          while (i < len) {
            out.collect(k(i))
            i += 1
          }
        }
      if (DebugReorderFunction.isDebug)
        println(s"REORDER: FLUSHED ${System.identityHashCode(this)}, idx: $idx, currentTS: $currentIdx maxTerm: ${maxOrderElem.mkString(",")}")
      out.collect(makeTerminator(idx))
      idx += 1
    }
    currentIdx = maxAgreedIdx
  }

  private def forceFlush(out: Collector[Fact]): Unit = {
    if (maxIdx != -1) {
      for (idx <- (currentIdx + 1) to maxIdx) {
        if (DebugReorderFunction.isDebug)
          println(s"REORDER: FORCE FLUSING ${System.identityHashCode(this)}, idx: $idx, currentTS: $currentIdx maxTerm: ${maxOrderElem.mkString(",")}")
        idx2Facts
          .remove(idx)
          .foreach { facts =>
            facts.foreach(out.collect)
            if (idx > maxIdx) {
              maxIdx = idx
              out.collect(makeTerminator(idx))
            }
          }
      }
    }
  }

  override def flatMap(value: (Int, Fact), out: Collector[Fact]): Unit = {
    val (subtaskidx, fact) = value
    if (currentIdx == -2) {
      require(fact.isMeta && fact.getName == "START")
      val firstIdx = fact.getArgument(0).asInstanceOf[String].toLong
      require(firstIdx >= 0)
      currentIdx = firstIdx - 1
      return
    }

    if (isOrderElement(fact)) {
      val idx = indexExtractor(fact)
      if (idx > maxOrderElem(subtaskidx))
        maxOrderElem(subtaskidx) = idx
      flushReady(out)
      return
    }

    if (fact.isMeta) {
      if (fact.getName == "EOF") {
        numEOF += 1
        if (numEOF == numSources) {
          forceFlush(out)
        }
        return
      } else if (fact.getName == "START")
        return
    }
    val idx = indexExtractor(fact)
    if (idx > maxIdx)
      maxIdx = idx

    if (idx < currentIdx) {
      throw new Exception("FATAL ERROR: Got a timestamp that should already be flushed")
    }
    insertElement(fact, subtaskidx, idx, out)
  }
}*/

/*class ReorderTotalOrderFunction extends ReorderFunction {
  private val tpTotsMap: mutable.LongMap[Long] = new mutable.LongMap[Long]()
  @transient private var tptots_state: ListState[(Long, Long)] = _

  override protected def isOrderElement(fact: Fact): Boolean = {
    val ret = fact.isTerminator
    if (ret) {
      val tp = fact.getTimepoint
      val ts = fact.getTimestamp
      tpTotsMap += (tp, ts)
    }
    ret
  }

  override protected def indexExtractor(fact: Fact): Long = fact.getTimepoint

  override protected def makeTerminator(idx: Long): Fact = Fact.terminator(tpTotsMap.remove(idx).get)

  /*override def snapshotState(context: FunctionSnapshotContext): Unit = {
    super.snapshotState(context)
    tpTotsMap.foreach(k => tptots_state.add(k))
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    super.initializeState(context)
    val desc_tptots = new ListStateDescriptor[(Long, Long)](
      "tptots map",
      TypeInformation.of(new TypeHint[(Long, Long)] {})
    )
    tptots_state = context.getOperatorStateStore.getListState(desc_tptots)
    if (context.isRestored) {
      tptots_state.get().forEach(k => tpTotsMap += (k._1, k._2))
    }
  }*/
}*/

/*class ReorderCollapsedPerPartitionFunction extends ReorderFunction {
  override protected def isOrderElement(fact: Fact): Boolean = fact.isTerminator

  override protected def indexExtractor(fact: Fact): Long = fact.getTimestamp

  override protected def makeTerminator(idx: Long): Fact = Fact.terminator(idx)
}*/

/*class ReorderCollapsedWithWatermarksFunction extends ReorderFunction {
  override protected def isOrderElement(fact: Fact): Boolean = fact.isMeta && fact.getName == "WATERMARK"

  override protected def indexExtractor(fact: Fact): Long = {
    if (isOrderElement(fact)) {
      fact.getArgument(0).asInstanceOf[String].toLong
    } else {
      fact.getTimestamp
    }
  }

  override protected def makeTerminator(idx: Long): Fact = Fact.terminator(idx)
}*/

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