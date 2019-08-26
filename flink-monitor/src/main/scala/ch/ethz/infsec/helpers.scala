package ch.ethz.infsec

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaConsumerBase}

object helpers {
  def createStringConsumerForTopic(topic: String, kafkaAddress: String, kafkaGroup: String): FlinkKafkaConsumerBase[String] = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", kafkaAddress)
    props.setProperty("group.id", kafkaGroup)
    val consumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema, props)
    consumer.setStartFromEarliest()
  }
}
