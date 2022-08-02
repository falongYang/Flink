package com.briup.test

import java.util.Properties

import com.briup.util.{MyEsUtil, MyKafkaUtil}
import com.dfec.kudu.kudusink1.SinkKudu
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011}

object testSourcekafka {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置启动检查点
    environment.enableCheckpointing(5000)
    // 设置事件时间为窗口
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.1.182:9092")
    properties.setProperty("group.id", "consumer-three")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "earliest")

    //创建一个source
    /**
      * (String topic, DeserializationSchema<T> valueDeserializer, Properties props)
      * (topic ,值的反序列化工具，Properties）
      *
      */
      val s: DataStreamSource[String] = environment.addSource(new FlinkKafkaConsumer010[String]("fj",new SimpleStringSchema(),properties))

//    val source: FlinkKafkaConsumer010[String] = MyKafkaUtil.getKafkaSource("estest")
//    val s: DataStreamSource[String] = environment.addSource(source)
    s.print("stream: ").setParallelism(1)

    //val sink: ElasticsearchSink[String] = MyEsUtil.getEsSink("es_flink")

    // 测试kudu

    //s.addSink(sink)

    environment.execute()
  }

}
