package com.geek.quickStart.es

import com.alibaba.fastjson.JSON

import org.apache.flink.api.common.functions.{MapFunction, RuntimeContext}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.{ElasticsearchSink, RestClientFactory}
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{Requests, RestClientBuilder}

import scala.math.BigDecimal.RoundingMode

object FactAppFlwDeviceInfoHisApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    listOperatorState(env)
    env.execute(this.getClass.getSimpleName)
  }

  def listOperatorState(env: StreamExecutionEnvironment): Unit ={
    env.readTextFile("./inputDir/flw_view_day").
      map(new MapFunction[String,(String,String)] {
      override def map(value: String): (String, String) = {
        val json=JSON.parseObject(value)
        val devId=json.getString("device_id")
        val createTime=json.getString("create_time")
        (devId,createTime)
      }
    }).addSink(getEsSink())

  }

  def getEsSink(): ElasticsearchSink[(String,String)] ={
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("yqdata000", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[(String,String)](
      httpHosts,
      new ElasticsearchSinkFunction[(String,String)] {
        def process(element: (String,String), ctx: RuntimeContext, indexer: RequestIndexer) {
          val map:java.util.HashMap[String, Object] = new java.util.HashMap[String, Object]
          val json = map
          json.put("id", element._1)
          json.put("device_id", element._1)
          json.put("last_visit_dt", element._2)

          val request: UpdateRequest = new UpdateRequest()
            .index("device_info_his_orc")
            .`type`("data").id(element._1).doc(json)
            .upsert(json)
          val rqst = request

          indexer.add(rqst)
        }
      }
    )

    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
    esSinkBuilder.setBulkFlushMaxActions(1)

    esSinkBuilder.build()

  }



}
