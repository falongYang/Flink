package com.briup.util

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests


object MyEsUtil {

  //存放EShosts的数组
  val httpHosts = new util.ArrayList[HttpHost]
  httpHosts.add(new HttpHost("192.168.1.170",9200,"http"))

  //获取ES Sink
  def getEsSink(indexName:String):ElasticsearchSink[String] = {
    val esfunc: ElasticsearchSinkFunction[String] = new ElasticsearchSinkFunction[String] {
      override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val str: JSONObject = JSON.parseObject(t)
        println("str===:" + str)
        val request: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(str)
        requestIndexer.add(request)
        println("保存1条")
      }
    }
    esfunc
    val sinkBuilder = new ElasticsearchSink.Builder[String](httpHosts,esfunc)
    // 刷新前缓冲的最大动作量
    sinkBuilder.setBulkFlushMaxActions(10)
    sinkBuilder.build()
  }


}
