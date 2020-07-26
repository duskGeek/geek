package com.geek.quickStart

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector

object WindowApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    reduceFunction(env)
    env.execute(this.getClass.getSimpleName)
  }

  def socketStream(env:StreamExecutionEnvironment): Unit ={
    val stream=env.socketTextStream("yqdata000",44444.toInt)
    val value= stream.map(x => {
      x.split(',')
    }).map(x=>{
      (x(0),x(1).toInt)
    }).keyBy(0)
      //.countWindow(5)
        .timeWindowAll(Time.seconds(5))

    value.
      sum(1).print()
  }

  def reduceFunction(env:StreamExecutionEnvironment): Unit ={

    val stream=env.socketTextStream("yqdata000",44444.toInt)
    val value= stream.map(x => {
      x.split(',')
    }).map(x=>{
      (x(0),x(1).toInt)
    }).keyBy(0)
      //.countWindow(5)
      .timeWindow(Time.seconds(5))

//    value.reduce((x,y)=>{
//      (x._1,x._2+y._2)
//    }).print()

    //value.aggregate(new MyAggFunction).print()

    value.process(new MyProcessFun).print()
  }


}

class MyAggFunction extends AggregateFunction[(String,Int),(Int,Int),Double]{
  override def createAccumulator(): (Int, Int) = (0,0)

  override def add(value: (String, Int), accumulator: (Int, Int)): (Int, Int) = {
    (value._2+accumulator._1,accumulator._2+1)
  }

  override def getResult(accumulator: (Int, Int)): Double = {
    accumulator._1/accumulator._2
  }

  override def merge(a: (Int, Int), b: (Int, Int)): (Int, Int) = ???
}

class MyProcessFun extends ProcessWindowFunction[(String,Int),(String,Int),Tuple,TimeWindow]{
  override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
    println("Key:"+key+"------------start-----------------")
    println("Key:"+key+" window start"+new Timestamp(context.window.getStart) + " window end"+new Timestamp(context.window.getEnd))

    var tmpEle=("",0)

    for(ele <- elements){
      tmpEle=(ele._1,tmpEle._2+ele._2)
    }
    out.collect(tmpEle)
    println("Key:"+key+"--------------end-------------")
  }
}