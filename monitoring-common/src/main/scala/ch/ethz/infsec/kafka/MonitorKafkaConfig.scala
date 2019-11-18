package ch.ethz.infsec.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.JavaConverters._

object MonitorKafkaConfig {
  private var initDone: Boolean = false
  private var topicName : String = "monitor_topic"
  private var groupName : String = "monitor"
  private var addr : String = "127.0.0.1:9092"

  def init(props: Properties) : Unit = {
    if (initDone)
      throw new Exception("KafkaConfig was already initialized")
    val tnGet = props.getProperty("topicName")
    val tn = if (tnGet == null) topicName else tnGet
    val gnGet = props.getProperty("groupName")
    val gn = if (gnGet == null) groupName else gnGet
    val laddrGet = props.getProperty("addr")
    val laddr = if (laddrGet == null) addr else laddrGet
    initInternal(tn, gn, laddr)
  }

  def init(
            topicName : String = topicName,
            groupName : String = groupName,
            addr : String = addr
          ) : Unit = {
    if (initDone)
      throw new Exception("KafkaConfig was already initialized")
    initInternal(topicName, groupName, addr)
  }

  private def initInternal(
            topicName : String,
            groupName : String,
            addr : String
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
