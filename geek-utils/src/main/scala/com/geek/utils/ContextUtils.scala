package com.geek.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object ContextUtils {
  def getConf(appName:String,master:String): SparkConf ={
    val conf=new SparkConf().setAppName(appName)
    if(master!=null && !master.isEmpty){
      conf.setMaster(master)
    }
    conf
  }

  def getSparkContext(appName:String,master:String): SparkContext ={
     new SparkContext(getConf(appName,master))
  }

  def getSparkSession(appName:String,master:String): SparkSession ={
    SparkSession.builder().config(getConf(appName:String,master:String)).enableHiveSupport().getOrCreate()
  }

  def getSparkStreamContext(appName:String,master:String,duration: Duration) : StreamingContext={
    new StreamingContext(
      getConf(appName,master).
        set("spark.streaming.kafka.consumer.poll.ms","100000"),
      duration)
  }

}
