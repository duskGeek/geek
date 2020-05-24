package com.geek.sparkstreaming.highRiskLog

import java.util.{Calendar, Date, Properties}

import com.geek.utils.mysql.MysqlConnect
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

private class BroadcastAlert {

  def getBroadcast(spark:SparkSession): Broadcast[ListBuffer[String]] ={
    val sc=spark.sparkContext
    val properties=new Properties()
    properties.setProperty("user",MysqlConnect.username)
    properties.setProperty("password",MysqlConnect.password)
    val df=spark.read.jdbc(MysqlConnect.url,"yqdata.alert_keys",properties)

    df.show()

    var list=new ListBuffer[String]()
    df.collectAsList().forEach(x=>{
      list.append(x.getString(0).toString())
    })
    sc.broadcast(list)
  }
}

object BroadcastAlert{
  private val broadcastAlert =new BroadcastAlert()
  var lastDate=new Date()
  private var broadcast :Broadcast[ListBuffer[String]]= null

  def getRiskWord(spark:SparkSession): ListBuffer[String] ={
    if(broadcast==null||(new Date().getTime-lastDate.getTime)>6000){
      if(broadcast!=null)
        broadcast.unpersist()

      broadcast=broadcastAlert.getBroadcast(spark)
      lastDate=new Date(System.currentTimeMillis())
    }
    broadcast.value
  }

}
