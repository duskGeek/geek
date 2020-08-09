package com.geek.quickStart.KafkaOperatorApp

import com.geek.utils.jedis.JedisManager
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import redis.clients.jedis.Jedis

object FlinkKafkaStateApp02 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val paramterTool=ParameterTool.fromPropertiesFile("conf/flink-job.properties")
    val kafkaStream=new CreateKafkaSource(env).create(paramterTool)

    kafkaStream.
      assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](
        Time.seconds(0)) {
        override def extractTimestamp(element: String): Long = {
          try{
            element.split(",")(0).toLong
          }catch {
            case e =>e.printStackTrace();
              0L
          }
        }
      }).
      map(x=>{
        try{
          val str=x.split(",")
          (str(1),str(2).toInt)
        }catch {
          case e =>e.printStackTrace();
            ("0",0)
        }
      }).filter(x=>{x._1 !="0" && x._2>0 }).keyBy(0).
      window(TumblingEventTimeWindows.of(Time.seconds(5))).
      sum(1)
      .addSink(new RedisRichSink)
      //.print()

    env.execute(this.getClass.getSimpleName)

    ///继续练习把计算结果输出到redis里
    //练习写redis用大小key方式
  }
}

class RedisRichSink() extends RichSinkFunction[(String, Int)] {
  var jedis: Jedis= _

  override def open(parameters: Configuration): Unit = {
    jedis=JedisManager.getRedis()
  }

  override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    println("hset : flink_kafka_wc "+value)
    jedis.hset("flink_kafka_wc",value._1,value._2.toString)
  }

  override def close(): Unit = {
    JedisManager.close(jedis)
  }

}
