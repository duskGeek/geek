package com.geek.utils.offsetManager

import com.geek.utils.jedis.JedisManager
import redis.clients.jedis.Jedis
import org.apache.kafka.common.TopicPartition

object KafkaOffsetManager {

  def saveOffset(jedis:Jedis,topic:String,groupId:String,offset:Map[String,String]): Unit ={
      try {
        jedis.select(0)
        import scala.collection.JavaConversions._
        jedis.hmset(getOffsetKey(topic,groupId),offset)
      }finally {
        JedisManager.close(jedis)
      }
  }

  def getOffset(topic:String,groupId:String, jedis:Jedis): Map[TopicPartition,Long] ={
    try {
          jedis.select(0)
          val offsetArray=jedis.hgetAll(getOffsetKey(topic,groupId))
          var offsetMap= Map[TopicPartition,Long]()
          import scala.collection.JavaConversions._
          offsetArray.foreach(x=>print("redies:",x._1,"   ",x._2))

          val offset=offsetArray.map(x=>{
            val topPartition=new TopicPartition(topic ,x._1.toInt)
            offsetMap += topPartition -> x._2.toLong
        })
        offsetMap
    }finally {
      JedisManager.close(jedis)
    }
  }

  def getOffsetKey(topic:String,groupId:String): String ={
    topic + "_" + groupId
  }

}
