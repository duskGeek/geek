package com.geek.quickStart.WaterMater

import java.sql.Timestamp
import java.util.Date

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WaterMark01App {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //waterMark(env)
    slidWindow(env)
    env.execute(this.getClass.getSimpleName)
  }

  def slidWindow(env:StreamExecutionEnvironment): Unit ={
    val stream=env.socketTextStream("yqdata000",44444.toInt).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
        override def extractTimestamp(element: String): Long = {
          element.split(',')(0).toLong
        }
      })
    val value= stream.map(x => {
      x.split(',')
    }).map(x=>{
      (x(1),x(2).toInt)
    }).keyBy(0).
      window(SlidingEventTimeWindows.of(Time.seconds(6),Time.seconds(2))).
      reduce(new ReduceFunction[(String,Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1,value2._2+value1._2)
        }
      },new ProcessWindowFunction[(String,Int),String,Tuple,TimeWindow]() {
        override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
          for(ele <- elements){
            out.collect(new Timestamp(context.window.getStart)+" "+
              new Timestamp(context.window.getEnd) +" "+
              new Timestamp(context.currentWatermark)+" "+
              ele+" ")
          }
        }
      }).print()


  }


  def waterMark(env:StreamExecutionEnvironment): Unit ={
    val stream=env.socketTextStream("yqdata000",44444.toInt)
    val outputTag=new OutputTag[(String,Int)]("late_tag")

    val value= stream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
      override def extractTimestamp(element: String): Long = {
        element.split(',')(0).toLong
      }
    }).map(x => {
      x.split(',')
    }).map(x=>{
      (x(1),x(2).toInt)
    }).keyBy(0).timeWindow(Time.seconds(5)).
      sideOutputLateData(outputTag).reduce(new ReduceFunction[(String,Int)] {
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        (value1._1,value2._2+value1._2)
      }
    },new ProcessWindowFunction[(String,Int),String,Tuple,TimeWindow]() {
      override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
        for(ele <- elements){
          out.collect(new Timestamp(context.window.getStart)+" "+
            new Timestamp(context.window.getEnd) +" "+
            new Timestamp(context.currentWatermark)+" "+
            ele+" ")
        }
      }
    })

    value.print()

    value.getSideOutput(outputTag).process(new ProcessFunction[(String,Int),String] {
      override def processElement(value: (String, Int), ctx: ProcessFunction[(String, Int), String]#Context, out: Collector[String]): Unit = {
        out.collect("it's late... "+
          new Timestamp(ctx.timestamp()) +" "+
          value+" ")
      }
    }).print()


  }
}

