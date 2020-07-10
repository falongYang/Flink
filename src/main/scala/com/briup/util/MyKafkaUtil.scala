package com.briup.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011}

object MyKafkaUtil {

  val prop = new Properties()
  prop.setProperty("bootstrap.servers","192.168.1.171:9092")
  prop.setProperty("group.id", "consumer-group")
  prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty("auto.offset.reset", "latest")

  def getKafkaSource(topic:String):FlinkKafkaConsumer010[String]={
    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop)
    kafkaConsumer
  }

}
