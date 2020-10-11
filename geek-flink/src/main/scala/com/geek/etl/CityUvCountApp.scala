package com.geek.etl

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object CityUvCountApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val ds=env.readTextFile("inputDir/log.txt")
    val wmDs=ds.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
      override def extractTimestamp(element: String): Long = {
        element.split(",")(3).toLong
      }
    }).map(x=>{
      val array=x.split(",")
      UserLog(array(0),array(1),array(2),array(3).toLong)
    })
    newAddUvCount(wmDs)
    env.execute(this.getClass.getSimpleName)
  }

  def uvCount(dataStream: DataStream[UserLog]): Unit ={
    dataStream.timeWindowAll(Time.minutes(1)).
      apply(new AllWindowFunction[UserLog,UVCount,TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[UserLog], out: Collector[UVCount]): Unit = {
          val uidSet=scala.collection.mutable.Set[String]()
          input.foreach(x=>{uidSet.add(x.userId) })
          out.collect(UVCount(window.getEnd.toString,uidSet.size.toLong))
        }
      }).print()
  }

  def newAddUvCount(dataStream: DataStream[UserLog]): Unit ={
    dataStream.timeWindowAll(Time.minutes(1)).
      aggregate(new AggregateFunction[UserLog,collection.immutable.Set[String],Long] {
      override def createAccumulator(): Set[String] = Set()

      override def add(value: UserLog, accumulator: Set[String]): Set[String] = accumulator+value.userId

      override def getResult(accumulator: Set[String]): Long = accumulator.size.toLong

      override def merge(a: Set[String], b: Set[String]): Set[String] = a++b
    },new AllWindowFunction[Long,UVCount,TimeWindow] {
      override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UVCount]): Unit = {
        input.foreach(num=>{out.collect(UVCount(window.getEnd.toString,num))  })
      }
    }).print()
  }
}

case class UVCount(time:String,num:Long)