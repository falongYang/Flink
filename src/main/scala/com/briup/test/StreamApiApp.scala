package com.briup.test

import com.briup.util.MyKafkaUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011}
import org.apache.flink.api.scala._

/**
  * 通过kafka消费者源 读取数据
  */
object StreamApiApp {
  def main(args: Array[String]): Unit = {
    // 1.env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.kafka Source

    val kafkaSource: FlinkKafkaConsumer010[String] = MyKafkaUtil.getKafkaSource("shopInfos")
    val value: DataStream[String] = env.addSource(kafkaSource)
   // val dStream: DataStream[String] = env.addSource(kafkaSource)

   // dStream.print()
    value.print()

    env.execute()

  }
}
