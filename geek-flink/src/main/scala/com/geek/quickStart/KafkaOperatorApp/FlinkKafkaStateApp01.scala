package com.geek.quickStart.KafkaOperatorApp

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.windowing.time.{Time => WindowTime}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows


object FlinkKafkaStateApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.enableCheckpointing(5000)
    val config=env.getCheckpointConfig
    config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    env.setStateBackend(new FsStateBackend("file:///./data"))
    //env.setStateBackend(new RocksDBStateBackend("hdfs://yqdata000:8020/test/ck"))
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      3, // 尝试重启的次数
      Time.of(10, TimeUnit.SECONDS) // 延时
    ))

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "yqdata000:9093,yqdata000:9094,yqdata000:9095")
    properties.setProperty("auto.offset.reset","earliest")
    properties.setProperty("enable.auto.commit","false")
    properties.setProperty("group.id", "FlinkKafkaState")

    //保证flink source精准一次消费
    val stream = env
      .addSource(new FlinkKafkaConsumer010[String]("fkstWm", new SimpleStringSchema(), properties)
        .setCommitOffsetsOnCheckpoints(false)).
      assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](
        WindowTime.seconds(0)) {
        override def extractTimestamp(element: String): Long = {
          try{
          element.split(",")(0).toLong
          }catch {
            case e =>e.printStackTrace();
              0L
          }
        }
      }).
      map(x=>{
      try{
        val str=x.split(",")
        (str(1),str(2).toInt)
      }catch {
        case e =>e.printStackTrace();
          ("0",0)
      }
    }).filter(x=>{x._1 !="0" && x._2>0 }).keyBy(0).
      window(TumblingEventTimeWindows.of(WindowTime.seconds(5))).
      sum(1)
      .print()

    env.execute(this.getClass.getSimpleName)
  }

}
