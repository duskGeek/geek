package com.geek.quickStart

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object CheckPointApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 每 1000ms 开始一次 checkpoint
    env.enableCheckpointing(3000);

    //easyCkeckPoint(env)

    keyedStreamCkeckPoint(env)
    env.execute(this.getClass.getSimpleName)

  }


  def easyCkeckPoint(env:StreamExecutionEnvironment): Unit ={
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      3, // 尝试重启的次数
      Time.of(10, TimeUnit.SECONDS) // 延时
    ))

    env.socketTextStream("yqdata000",44444).map(x=>{
      if(x=="yq")
        throw new Exception("报错了！！！！！！！")
      x
    }).print()
  }


  def keyedStreamCkeckPoint(env:StreamExecutionEnvironment): Unit ={
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      3, // 尝试重启的次数
      Time.of(10, TimeUnit.SECONDS) // 延时
    ))

    import com.geek.quickStart.WindowAverageMapState

    env.socketTextStream("yqdata000",44444).map(x=>{
      if(x=="yq")
        throw new Exception("报错了！！！！！！！")
      x
    }).map(x=>{
      val words=x.split(",")
      (words(0).toLong,words(1))
    }).keyBy(0).flatMap(new WindowAverageMapState2).print()
  }



}
