package com.geek.quickStart

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import com.geek.quickStart.SourceFunctions.{Access, ListOutFunction}
import org.apache.flink.streaming.api.windowing.time.Time


object SourceFunctionJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource[Access](new ListOutFunction()).
      keyBy("domain").
      timeWindow(Time.seconds(5)).
      sum("traffic").print()


    env.execute(this.getClass.getSimpleName)
  }

}
