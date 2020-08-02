package com.geek.quickStart.WaterMater

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object WaterMark02App {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    waterMarkFun(env)

    env.execute(this.getClass.getSimpleName)
  }

  def waterMarkFun(env:StreamExecutionEnvironment): Unit ={
    val stream=env.socketTextStream("yqdata000",44444.toInt)
    val outputTag=new OutputTag[(String,Int)]("late_tag")
    val value= stream.
      assignTimestampsAndWatermarks(new MyPriodAssignTimestampAndWatermarks(Time.seconds(5).toMilliseconds))
      .map(x => {
        x.split(',')
      }).map(x=>{
      (x(1),x(2).toInt)
    }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print()

  }

}

class MyPriodAssignTimestampAndWatermarks(allowDelayTime:Long) extends AssignerWithPeriodicWatermarks[String]{

  var maxTimeStamp= 0L

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTimeStamp-allowDelayTime)
  }

  override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
    val nowTime=element.split(",")(0).toLong
    maxTimeStamp=maxTimeStamp.max(nowTime)
    println("nowTime: "+new Timestamp(nowTime) +
      " previousTime"+new Timestamp(previousElementTimestamp)+
      " maxTimeStamp"+new Timestamp(maxTimeStamp)+
      " Watermark"+getCurrentWatermark.getTimestamp)

    nowTime
  }
}