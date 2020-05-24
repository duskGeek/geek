package com.geek.sparkstreaming.xxxoffsetmamager

import com.geek.dao.xxxoffsetmamager.XxxOffsetManagerDAO
import com.geek.utils.ContextUtils
import com.geek.utils.jedis.JedisManager
import com.geek.utils.mysql.MysqlConnectPool
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable.ListBuffer

object XxxOffsetManager {
  val slide_interval=Seconds(5)

  def main(args: Array[String]): Unit = {
    val ssc=ContextUtils.getSparkStreamContext(this.getClass.getSimpleName,"local[2]",slide_interval)

    val groupId="offsetManagerHW"

    val kafkaParams= Map[String, Object](
      "bootstrap.servers" -> "yqdata000:9093,yqdata000:9094,yqdata000:9095",
      "key.deserializer" ->  classOf[StringDeserializer],
      "value.deserializer" ->  classOf[StringDeserializer],
      "group.id" ->  groupId,
      "auto.offset.reset" ->  "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("wordcount")

    val jedis=JedisManager.getRedis()
    jedis.select(0)
    val offsetArray=jedis.hgetAll(topics(0) + "_" + groupId)
    JedisManager.close(jedis)

    var offsetMap= Map[TopicPartition,Long]()

    import scala.collection.JavaConversions._

    offsetArray.foreach(x=>print("redies:",x._1,"   ",x._2))

    val offset=offsetArray.map(x=>{
      val topPartition=new TopicPartition(topics(0) ,x._1.toInt)
      offsetMap += topPartition -> x._2.toLong
    })

    val kafkaDs=KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams, offsetMap)
    )

    MysqlConnectPool.yqdataPoolSetup()

    val offsetDAO=new XxxOffsetManagerDAO()

    kafkaDs.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        val offsetRDD=rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val wc=rdd.map(_.value()).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)

        val jedis=JedisManager.getRedis()
        try{
          wc.foreachPartition(x=>{
            val listBuffer = new ListBuffer[Seq[Any]]()
            x.foreach(x=>{
              listBuffer+=Seq(x._1,x._2)
            })
            offsetDAO.bacthInsert(listBuffer)
          })

          var offsetEnd=Map[String,String]()
          offsetRDD.foreach(x=>
            offsetEnd += x.partition.toString -> x.untilOffset.toString
          )
          jedis.hmset(topics(0) + "_" + groupId,offsetEnd)
        }catch {
          case exception: Exception=> exception.printStackTrace()
        }finally {
          jedis.close()
        }
      }else{
        println("###################当前批次没有数据#####################")
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
    MysqlConnectPool.yqdataPoolColse()

  }

}
