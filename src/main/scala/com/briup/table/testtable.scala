package com.briup.table

import com.alibaba.fastjson.JSON
import com.briup.util.MyKafkaUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment


object testtable {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val myKafkaConsumer: FlinkKafkaConsumer010[String] = MyKafkaUtil.getKafkaSource("estest")

    val dstream: DataStream[String] = env.addSource(myKafkaConsumer)

    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val startuplog: DataStream[StartupLog] = dstream.map{string => JSON.parseObject(string,classOf[StartupLog])}

    val table: Table = tableEnv.fromDataStream(startuplog)

    val tableresult: Table = table.select("id,name").filter("name='zhangsan'")

    tableresult


  }

}


case class StartupLog()