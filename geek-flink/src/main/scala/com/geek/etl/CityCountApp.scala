package com.geek.etl

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object CityCountApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val ds=env.readTextFile("inputDir/log.txt")
    ds.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
      override def extractTimestamp(element: String): Long = {
        element.split(",")(3).toLong
      }
    }).map(x=>{
      val array=x.split(",")
      UserLog(array(0),array(1),array(2),array(3).toLong)
    }).keyBy(x=>x.city).
      timeWindow(Time.minutes(1),Time.seconds(10)).
      aggregate(new AggregateFunction[UserLog,Long,Long] {
        override def createAccumulator(): Long = 0L

        override def add(value: UserLog, accumulator: Long): Long = accumulator+1

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a+b
      }, new WindowFunction[Long,CityCountUv,String,TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CityCountUv]): Unit = {
          input.foreach(uv=>{
            out.collect(CityCountUv(key,uv,window.getEnd))
          })
        }
      }).print()

    env.execute(this.getClass.getSimpleName)
  }

}

case class UserLog(userId:String,city:String,domain:String,time:Long)

case class CityCountUv(city:String,num:Long,endTime:Long)