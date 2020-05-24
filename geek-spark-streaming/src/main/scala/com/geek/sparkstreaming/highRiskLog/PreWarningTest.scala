package com.geek.sparkstreaming.highRiskLog

import com.alibaba.fastjson.JSON
import com.geek.utils.ContextUtils
import com.geek.utils.InfluxDB.InfluxDBUtils
import com.geek.utils.jedis.JedisManager
import com.geek.utils.offsetManager.KafkaOffsetManager
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.influxdb.{InfluxDB, InfluxDBFactory}

import scala.collection.mutable.ListBuffer


object PreWarningTest {

  val dbName="yqdata"
  val slide_interval=Seconds(5)
  val window_interval=Seconds(5)
  val batch_size=3

  def main(args: Array[String]): Unit = {

    val groupId="PREWARNING_consumer1"

    val spark=ContextUtils.getSparkSession("PreWarningTestc2","local[2]")
    val sc=spark.sparkContext
    val ssc=new StreamingContext( sc, slide_interval)

    val kafkaParams= Map[String, Object](
      "bootstrap.servers" -> "yqdata000:9093,yqdata000:9094,yqdata000:9095",
      "key.deserializer" ->  classOf[StringDeserializer],
      "value.deserializer" ->  classOf[StringDeserializer],
      "group.id" ->  groupId,
      "auto.offset.reset" ->  "earliest",
      "enable.auto.commit" -> "false",//是否自动确认offset
      //"auto.offset.reset"-> "latest",
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
      Subscribe[String, String](topics, kafkaParams,KafkaOffsetManager.
        getOffset(topics(0),groupId ,JedisManager.getRedis()))
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
    })

    val windowDs=filterds.window(window_interval,slide_interval)

    var offset=Map[String,String]()
    dstream.foreachRDD(rdd=>{
      val offsetRDD=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRDD.foreach(offSetRange=>{
        offset += offSetRange.partition.toString -> offSetRange.untilOffset.toString
      })
    })

      windowDs.foreachRDD(rdd =>{
        if(rdd.isEmpty()){
          println("##############当前批次没有数据##########################")
          return
        }
        val newRdd=rdd.filter(_!=null)
        newRdd.context

       val riskWord = BroadcastAlert.getRiskWord(spark)

      import spark.implicits._
      val df=newRdd.toDF()

      val infoLog=df.select('hostName,'logType,'serviceName)

        var wheresql=""
        if(riskWord!=null&&riskWord.size>0){
          riskWord.foreach(x=>wheresql+=" logInfo like '%"+x+"%' or ")
          wheresql=wheresql.substring(0,wheresql.length-2)
        }else{
          wheresql=" 1=2  "
        }
        val alertLogdf=df.where(wheresql).selectExpr(" hostName "," 'alter' AS logType " ," serviceName " )
        val result=infoLog.union(alertLogdf).
          groupBy('hostName,'logType,'serviceName).count()

        try {
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
                  this.insertflux(influxDB,dbName,rp,listBuffer)
                }
              })
              if(listBuffer.size>0){
                this.insertflux(influxDB,dbName,rp,listBuffer)
              }
            }
          })
        KafkaOffsetManager.saveOffset(JedisManager.getRedis(),topics(0),groupId,offset)
      }catch {
        case e =>e.printStackTrace()
      }
    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  def insertflux(influxDB:InfluxDB,dbName:String ,rp:String,listBuffer:ListBuffer[String]): Unit ={
    var value=""
    listBuffer.foreach(x=>value+=x)
    println("##############################################"+value)
    val insertValue = value.substring(0, value.length)
    influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, value)
    listBuffer.clear()
  }

}

case class CDHRoleLog( hostName: String , serviceName: String ,lineTimestamp: String ,
                       logType: String , logInfo: String)