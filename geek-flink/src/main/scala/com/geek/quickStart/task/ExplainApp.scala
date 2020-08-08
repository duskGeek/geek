package com.geek.quickStart.task

import com.geek.quickStart.SourceFunctions.{Access, ListOutFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ExplainApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    showExplain(env)

    env.execute(this.getClass.getSimpleName)
  }

  def showExplain(env:StreamExecutionEnvironment): Unit ={
    val stream=env.socketTextStream("yqdata000",44444.toInt).flatMap(x => {
      x.split(',')
    }).filter(!_.equals("a"))
      //.startNewChain()
      //.disableChaining()
      .slotSharingGroup("MyNewSoltGroup")
      .map(x=>{
      (x,1)
    }).keyBy(0).sum(1)

    println(env.getExecutionPlan)
    stream.print()
  }

  def showExplain2(env:StreamExecutionEnvironment): Unit ={
    env.fromCollection(List(
      (1L, "buy"),
      (1L, "add"),
      (1L, "fav"),
      (1L, "fav"),
      (1L, "buy"),
      (1L, "buy"),
      (2L, "add"),
      (2L, "buy"),
      (2L, "buy"),
      (2L, "buy"),
    )).map(x=>{
      (x._1,x._2,1)
    }).keyBy(1).sum(0).print().setParallelism(1)
  }

  def showExplain3(env:StreamExecutionEnvironment): Unit ={
    env.addSource[Access](new ListOutFunction()).map(x=>{x}).
      keyBy("domain").
      timeWindow(Time.seconds(5)).
      sum("traffic").print().setParallelism(1)
  }


}
