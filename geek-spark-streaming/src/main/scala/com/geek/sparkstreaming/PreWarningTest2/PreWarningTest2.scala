package com.geek.sparkstreaming.PreWarningTest2


import com.alibaba.fastjson.JSON
import com.geek.sparkstreaming.highRiskLog.BroadcastAlert
import com.geek.utils.ContextUtils
import com.geek.utils.InfluxDB.InfluxDBUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.influxdb.{InfluxDB, InfluxDBFactory}

import scala.collection.mutable.ListBuffer


object PreWarningTest2 {

  def genStructType () :StructType ={
    StructType(
      new StructField("hostName", StringType, true)::
      new StructField("serviceName", StringType, true)::
      new StructField("lineTimestamp", StringType, true)::
      new StructField("logType", StringType, true)::
      new StructField("logInfo", StringType, true)
        ::Nil
    )
  }

  val dbName="yqdata"
  val slide_interval=Seconds(5)
  val window_interval=Seconds(5)
  val batch_size=3

  def main(args: Array[String]): Unit = {

    val ssc=ContextUtils.getSparkStreamContext("PreWarningTestc2","local[2]", slide_interval)



    val kafkaParams= Map[String, Object](
      "bootstrap.servers" -> "yqdata000:9093,yqdata000:9094,yqdata000:9095",
      "key.deserializer" ->  classOf[StringDeserializer],
      "value.deserializer" ->  classOf[StringDeserializer],
      "group.id" ->  "PREWARNING_consumer1",
      "auto.offset.reset" ->  "earliest",
      "enable.auto.commit" -> "false",
      //"auto.offset.reset"-> "latest",
     // "enable.auto.commit"-> false, //是否自动确认offset
      "max.partition.fetch.bytes" -> int2Integer(10485760),
      //"request.timeout.ms" -> int2Integer(210000),
      //"session.timeout.ms" -> int2Integer(180000),
      //"heartbeat.interval.ms" -> int2Integer(30000),
      "receive.buffer.bytes" -> int2Integer(10485760)
    )
    val topics = Array("PREWARNING")
   
    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val filterds=dstream.map(x=>{
      val value=x.value()
      try{
        val strobj=JSON.parseObject(value)
        CDHRoleLog(strobj.getString("hostname"), strobj.getString("servicename"), strobj.getString("time"),
          strobj.getString("logtype"), strobj.getString("loginfo"))
      }catch {
        case e => e.printStackTrace(); null
      }
    }).filter(_!=null)

    val windowDs=filterds.window(window_interval,slide_interval)

    windowDs.foreachRDD(rdd =>{

      if(rdd.isEmpty()){
        println("##############当前批次没有数据##########################")
        return
      }

      val spark=SparkSession.builder.config(rdd.context.getConf).getOrCreate
      import spark.implicits._
      val df=rdd.toDF()

      val result=df.select('hostName,'logType,'serviceName).
        groupBy('hostName,'logType,'serviceName).count()

//      result.show(10)
      result.rdd.coalesce(2).foreachPartition(rddPartition=>{
        val influxDB=InfluxDBFactory.connect("http://"+InfluxDBUtils.getInfluxIP+":"+InfluxDBUtils.getInfluxPORT(true))
        val rp=InfluxDBUtils.defaultRetentionPolicy(influxDB.version())

        if(!rddPartition.isEmpty){
          val listBuffer=new ListBuffer[String]()
          val batchText=rddPartition.map(row=>{
            val key=row.getString(0)+"_"+row.get(2)+"_"+row.get(1)
            "prewarning,host_service_logType="+key+" count="+row.get(3)+"\n"
          })

          batchText.foreach(text=>{
            listBuffer.append(text)
            if(listBuffer.size==batch_size){
              var value=""
              listBuffer.foreach(x=>value+=x)
              println("##############################################"+value)
              value = value.substring(0, value.length)
              influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, value)
              listBuffer.clear()
            }
          })
          if(listBuffer.size>0){
            var value=""
            listBuffer.foreach(x=>value+=x)
            println("##############################################"+value)
            value = value.substring(0, value.length)
            influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, value)
          }
        }
      })

    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}

case class CDHRoleLog( hostName: String , serviceName: String ,lineTimestamp: String ,
                       logType: String , logInfo: String)