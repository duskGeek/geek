package com.geek.quickStart

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import com.geek.quickStart.SourceFunctions.{Access, ListOutFunction}
import com.geek.quickStart.partitionCustom.MyPartition
import org.apache.flink.streaming.api.windowing.time.Time


object SourceFunctionJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    partitionCustomFun(env)


    env.execute(this.getClass.getSimpleName)
  }

  def partitionCustomFun(env:StreamExecutionEnvironment): Unit ={
    env.addSource[Access](new ListOutFunction()).
      partitionCustom(new MyPartition(),"domain").
      map(x=>{
        (x.domain,x.traffic,x.time)
      })
  .print()

  }

  def sourceFun(env:StreamExecutionEnvironment): Unit ={
    env.addSource[Access](new ListOutFunction()).
      keyBy("domain").
      timeWindow(Time.seconds(5)).
      sum("traffic").print()
  }

}
