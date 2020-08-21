package ch.ethz.infsec.kafka

import java.util.{Collections, Properties}
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.JavaConverters._

object MonitorKafkaConfig {
  private var initDone: Boolean = false
  private var topicName : String = "monitor_topic"
  private var groupName : String = "monitor"
  private var addr : String = "127.0.0.1:9092"
  private var numPartitions: Option[Int] = None

  def init(props: Properties) : Unit = {
    if (initDone)
      throw new Exception("KafkaConfig was already initialized")
    val tnGet = props.getProperty("topicName")
    val tn = if (tnGet == null) topicName else tnGet
    val gnGet = props.getProperty("groupName")
    val gn = if (gnGet == null) groupName else gnGet
    val laddrGet = props.getProperty("addr")
    val laddr = if (laddrGet == null) addr else laddrGet
    val clearGet = props.getProperty("clearTopic")
    val clear = if (clearGet == null) false else clearGet.toBoolean
    val numPartGet = props.getProperty("numPartitions")
    val numPart = if (numPartGet == null) None else Some(numPartGet.toInt)
    initInternal(tn, gn, laddr, clear, numPart)
  }

  def init(
            topicName : String = topicName,
            groupName : String = groupName,
            addr : String = addr,
            clearTopic: Boolean = false,
            numPartitions: Option[Int] = None
          ) : Unit = {
    if (initDone)
      throw new Exception("KafkaConfig was already initialized")
    initInternal(topicName, groupName, addr, clearTopic, numPartitions)
  }

  private def initInternal(
            topicName : String,
            groupName : String,
            addr : String,
            clearTopic: Boolean,
            numPartitions: Option[Int]
          ) : Unit = {
    MonitorKafkaConfig.topicName = topicName
    MonitorKafkaConfig.groupName = groupName
    MonitorKafkaConfig.addr = addr


    if (clearTopic) {
      val admin = AdminClient.create(MonitorKafkaConfig.getKafkaPropsInternal)
      val res = admin.deleteTopics(Collections.singletonList(MonitorKafkaConfig.getTopicInternal))
      Thread.sleep(1000)
      try {
        res.all().get(10, TimeUnit.SECONDS)
      } catch {
        case _: Throwable => println("Kafka topic does not exist, creating it")
      }
      numPartitions match {
        case Some(n) =>
          val res_create = admin.createTopics(Collections.singletonList(new NewTopic(topicName, n, 1)))
          res_create.all().get(10, TimeUnit.SECONDS)
          admin.close(10, TimeUnit.SECONDS)
          MonitorKafkaConfig.numPartitions = numPartitions
        case None =>
          throw new Exception("if clearTopic is set, numPartitions must be set")
      }
    }
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
    //props.setProperty("buffer.memory", "3355443200")
    //props.setProperty("batch.size", "30384000")
    //props.setProperty("max.partition.fetch.bytes", "104857600")
    //props.setProperty("fetch.max.bytes", "1048576000")
    //props.setProperty("compression.type", "none")
    //props.setProperty("linger.ms", "300")
    //props.setProperty("receive.buffer.bytes", "3276800")
    //props.setProperty("send.buffer.bytes", "13107200")
    //props.setProperty("max.in.flight.requests.per.connection", "200")
    //props.setProperty("acks", "0")
    props
  }

  private def getNumPartitionsInternal : Int = {
    numPartitions match {
      case None =>
        val tmp_producer = new KafkaProducer[String, String](MonitorKafkaConfig.getKafkaProps)
        val n = tmp_producer.partitionsFor(topicName).size()
        /*if (n < 2) {
          throw new Exception("ERROR: # partitions is less than 2")
        }*/
        n
      case Some(n) => n
    }
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
