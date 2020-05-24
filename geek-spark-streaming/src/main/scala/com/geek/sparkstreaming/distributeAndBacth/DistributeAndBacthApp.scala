package com.geek.sparkstreaming.distributeAndBacth

import com.alibaba.fastjson.JSON
import com.geek.dao.distributeAndBacth.DistributeAndBacthDDao
import com.geek.sparkstreaming.highRiskLog.BroadcastAlert
import com.geek.utils.ContextUtils
import com.geek.utils.InfluxDB.InfluxDBUtils
import com.geek.utils.jedis.JedisManager
import com.geek.utils.mysql.MysqlConnectPool
import com.geek.utils.offsetManager.KafkaOffsetManager
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.influxdb.{InfluxDB, InfluxDBFactory}

import scala.None
import scala.collection.mutable.ListBuffer


object DistributeAndBacthApp {

  val dbName="yqdata"
  val slide_interval=Seconds(5)
  val window_interval=Seconds(5)
  val batch_size=3
  val groupId="ORDER_consumer1"

  def main(args: Array[String]): Unit = {

    val ssc=ContextUtils.getSparkStreamContext(this.getClass.getSimpleName,"local[2]", slide_interval)



    val kafkaParams= Map[String, Object](
      "bootstrap.servers" -> "yqdata000:9093,yqdata000:9094,yqdata000:9095",
      "key.deserializer" ->  classOf[StringDeserializer],
      "value.deserializer" ->  classOf[StringDeserializer],
      "group.id" ->  "ORDER_consumer1",
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
    val topics = Array("ORDER")
   
    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val filterds=dstream.map(x=>{
      val value=x.value()
      try{
        val strobj=value.split(",")
        orderInfo(strobj(0),BigDecimal.apply(strobj(1)),strobj(2).toInt)
      }catch {
        case e => e.printStackTrace(); null
      }
    }).filter(_!=null)

    var offsetMap=Map[String,String]()
    dstream.foreachRDD(rdd=>{
      val offsetsRdd=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetsRdd.foreach(offset=>{
        offsetMap += offset.partition.toString -> offset.untilOffset.toString
      })
    })

    filterds.foreachRDD(rdd =>{
      if(rdd.isEmpty()){
        println("##############当前批次没有数据##########################")
        return
      }
//      result.show(10)
      rdd.coalesce(2).foreachPartition(rddPartition=>{
        if(!rddPartition.isEmpty){
          try{
            MysqlConnectPool.yqdataPoolSetup()
            val dao = new DistributeAndBacthDDao[orderInfo]()
            var resultMap = Map[Int,ListBuffer[orderInfo]]()
            rddPartition.foreach(orderInfo=>{
              var listBuffer=resultMap.get(orderInfo.catagory).getOrElse(new ListBuffer[orderInfo]())

              listBuffer.append(orderInfo)
              if(listBuffer.size==batch_size){
                dao.bacthInsert(
                  this.insertBacthOrder(listBuffer,this.tableMap.get(orderInfo.catagory).getOrElse("order"))
                )
                listBuffer.clear()
              }
              resultMap+= orderInfo.catagory->listBuffer
            })
            dao.bacthInsert(this.insertBacthOrder(resultMap))

            KafkaOffsetManager.saveOffset(JedisManager.getRedis(),topics(0),groupId,offsetMap)
          }finally {
            MysqlConnectPool.yqdataPoolColse()
          }
        }
      })
    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  def insertBacthOrder(listBuffer: ListBuffer[orderInfo],tableName:String):  ListBuffer[Seq[Any]] ={
    val insertSeq=new ListBuffer[Seq[Any]]()
    listBuffer.foreach(order=>{
      insertSeq+= Seq(order.orderId,order.money,order.catagory)
    })
    insertSeq
  }

  def insertBacthOrder(result:Map[Int,ListBuffer[orderInfo]]): Map[String,ListBuffer[Seq[Any]]] ={
    result.map(entry=>{
      val listBuffer=entry._2
      if(listBuffer.size>0){
        val insertSeq=new ListBuffer[Seq[Any]]()
        listBuffer.foreach(order=>{
          insertSeq+= Seq(order.orderId,order.money,order.catagory)
        })
        (this.tableMap.get(entry._1).getOrElse("order"),insertSeq)
      }else{
        null
      }
    }).filter(_!=null)
  }

  val tableMap=Map(
    1-> "order1",
    2-> "order2",
    3-> "order3",
    4-> "order4"
  )
}

case class orderInfo(orderId:String,money:BigDecimal,catagory:Int)