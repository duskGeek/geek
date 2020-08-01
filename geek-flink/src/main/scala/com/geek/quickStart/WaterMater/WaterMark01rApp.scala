package com.geek.quickStart.WaterMater

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WaterMark01rApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    waterMark(env)

    env.execute(this.getClass.getSimpleName)
  }

  def waterMark(env:StreamExecutionEnvironment): Unit ={
    val stream=env.socketTextStream("yqdata000",44444.toInt)
    val outputTag=new OutputTag[(String,Int)]("late_tag")

    val value= stream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(5)) {
      override def extractTimestamp(element: String): Long = {
        element.split(',')(0).toLong
      }
    }).map(x => {
      x.split(',')
    }).map(x=>{
      (x(1),x(2).toInt)
    }).keyBy(0).timeWindow(Time.seconds(5)).
      sideOutputLateData(outputTag)



    value.sum(1).print()

    value.


  }
}
