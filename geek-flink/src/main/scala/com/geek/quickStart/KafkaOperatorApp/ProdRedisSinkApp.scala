package com.geek.quickStart.KafkaOperatorApp

import com.geek.utils.jedis.JedisCustomerManager
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import redis.clients.jedis.Jedis

object ProdRedisSinkApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val paramterTool=ParameterTool.fromPropertiesFile("conf/flink-job.properties")
    env.getConfig.setGlobalJobParameters(paramterTool)

    dayProvinceDomainSum(env)
    env.execute(this.getClass.getSimpleName)
  }

  def dayProvinceSum(env:StreamExecutionEnvironment): Unit ={
    env.fromElements(log("2020-08-01","huanan","cs","www.baidu.com",100),
      log("2020-08-01","huanan","hy","www.alibaba.com",200),
      log("2020-08-01","beijing","beijing","www.qq.com",120),
      log("2020-08-01","hubei","wh","www.alibaba.com",200),
      log("2020-08-01","huanan","hy","www.youku.com",200),
      log("2020-08-01","guangdong","gz","www.alibaba.com",300),
      log("2020-08-01","hubei","wh","www.alibaba.com",300)).
      map(new MapFunction[log,(String,String,Int)] {
        override def map(value: log): (String, String, Int) = {
          ("flink_traffic_"+value.day,value.province,value.traffic.toInt)
        }
      }).keyBy(0,1).sum(2)
      .addSink(new LogRedisSink)
  }

  def dayProvinceDomainSum(env:StreamExecutionEnvironment): Unit ={
    env.fromElements(log("2020-08-01","huanan","cs","www.baidu.com",100),
      log("2020-08-01","huanan","hy","www.alibaba.com",200),
      log("2020-08-01","beijing","beijing","www.qq.com",120),
      log("2020-08-01","hubei","wh","www.alibaba.com",200),
      log("2020-08-01","huanan","hy","www.youku.com",200),
      log("2020-08-01","guangdong","gz","www.alibaba.com",300),
      log("2020-08-01","guangdong","gz","www.alibaba.com",530),
      log("2020-08-01","hubei","wh","www.alibaba.com",300)).
      map(new MapFunction[log,(String,String,Int)] {
        override def map(value: log): (String, String, Int) = {
          ("flink_traffic_"+value.day+"_"+value.domain,value.province,value.traffic.toInt)
        }
      }).keyBy(0,1).sum(2)
      .addSink(new LogRedisSink)
  }
}

class LogRedisSink extends RichSinkFunction[(String,String,Int)]{
  var jedis:Jedis = _

  override def open(parameters: Configuration): Unit = {
    val parameters=getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]

    val host=parameters.getRequired("redis.host")
    val port=parameters.getRequired("redis.port").toInt
    val timeout=parameters.getInt("redis.timeout",2000)
    val db=parameters.getInt("redis.db",0)

    jedis=JedisCustomerManager.getRedis(host,port,timeout)
    jedis.select(db)
  }

  override def invoke(value: (String,String,Int)): Unit = {
    if(!jedis.isConnected){
      jedis.connect()
    }
    jedis.hset(value._1,value._2,value._3+"")
  }

  override def close(): Unit = {
    JedisCustomerManager.close(jedis)
  }
}

case class log(day:String,province:String,city:String,domain:String,traffic:Long)