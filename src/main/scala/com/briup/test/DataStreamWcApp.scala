package com.briup.test

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

// 实时
object DataStreamWcApp {
  def main(args: Array[String]): Unit = {

    // 构建环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val dataStream : DataStream[String] =
      env.socketTextStream("192.168.1.174",7777)

    val sumDstream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)//
      .sum(1)

    sumDstream.print()
    env.execute()


  }
}
