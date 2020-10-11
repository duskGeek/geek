package com.geek.etl

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random


object CityPvCountApp {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val ds=env.readTextFile("inputDir/log.txt")
    val wmDs=ds.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
      override def extractTimestamp(element: String): Long = {
        element.split(",")(3).toLong
      }
    })
    //pvCount(wmDs)
    skewPvCount(wmDs)
    env.execute(this.getClass.getSimpleName)
  }

  def pvCount(dataStream: DataStream[String]): Unit ={
    dataStream.map(x=>{
      ("pv",1L)
    }).keyBy(x=>x._1).timeWindow(Time.minutes(1),Time.seconds(10)).
      aggregate(new AggregateFunction[(String,Long),Long,Long] {
        override def createAccumulator(): Long = 0L

        override def add(value: (String, Long), accumulator: Long): Long = {
          value._2+accumulator
        }
        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a+b
      },new WindowFunction[Long,PVCount,String,TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PVCount]): Unit = {
          input.foreach(x=>{
            out.collect(PVCount(window.getEnd,x,key))
          })
        }
      }).print()
  }

  def skewPvCount(dataStream: DataStream[String]): Unit ={
    dataStream.map(x=>(Random.nextInt(10)+"pv",1L)).keyBy(_._1).
      timeWindow(Time.minutes(1)).
      aggregate(new AggregateFunction[(String,Long),Long,Long] {
        override def createAccumulator(): Long = 0L

        override def add(value: (String, Long), accumulator: Long): Long = {
          value._2+accumulator
        }
        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a+b
      },new WindowFunction[Long,PVCount,String,TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PVCount]): Unit = {
          input.foreach(x=>{
            out.collect(PVCount(window.getEnd,x,key))
          })
        }
      }).map(pvCount=>{
      PVCount(pvCount.endTime,pvCount.num,pvCount.name.takeRight(2))
    }).timeWindowAll(Time.seconds(1)).sum("num").print()

//      .keyBy(x=>{
//      (x.endTime)
//    }).sum("num").print()
  }
}

case class PVCount(endTime:Long,num:Long,name:String)