package com.briup.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011}

object MyKafkaUtil {

  //配置参数
  val prop = new Properties()
  prop.setProperty("bootstrap.servers","192.168.1.171:9092")
  prop.setProperty("group.id", "consumer-group")
  prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty("auto.offset.reset", "latest")

  //根据参数，构建flinkkafka消费者
  def getKafkaSource(topic:String):FlinkKafkaConsumer010[String]={
    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop)
    kafkaConsumer
  }

}
