package com.geek.quickStart

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object CheckPointApp {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","hadoop")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 每 1000ms 开始一次 checkpoint
    env.enableCheckpointing(3000);
    val config=env.getCheckpointConfig
    config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    env.setStateBackend(new FsStateBackend("hdfs://yqdata000:8020/test/ck"))

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
