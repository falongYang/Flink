package com.briup.test

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object DateSetWcApp {
  def main(args: Array[String]): Unit = {
    // 1.env   // 2.source  // 3.transform  // 4.sink


    // 参数
    val tool = ParameterTool.fromArgs(args)
    val inputpath = tool.get("input")
    val outputpath = tool.get("output")

    // 1.环境对象
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val txtDateSet: DataSet[String] = env.readTextFile(inputpath)


    val aggSet: AggregateDataSet[(String, Int)] = txtDateSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    aggSet.writeAsCsv(outputpath).setParallelism(1)
    //aggSet.print()
    env.execute()


  }
}
