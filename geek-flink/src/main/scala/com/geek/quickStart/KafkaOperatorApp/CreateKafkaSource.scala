package com.geek.quickStart.KafkaOperatorApp

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.scala._

class CreateKafkaSource(env: StreamExecutionEnvironment) {

  def create(paramTool:ParameterTool) ={

    env.enableCheckpointing(paramTool.getInt("flink.checkpointing.interval"))
    val config=env.getCheckpointConfig
    config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    env.setStateBackend(new FsStateBackend(paramTool.getRequired("flink.backend.location")))
    //env.setStateBackend(new RocksDBStateBackend("hdfs://yqdata000:8020/test/ck"))
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      paramTool.getInt("flink.restart.attempts",3), // 尝试重启的次数
      paramTool.getLong("flink.delay.interval",Time.of(10, TimeUnit.SECONDS).toMilliseconds) // 延时
    ))

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", paramTool.getRequired("kafka.bootstrap.servers"))
    properties.setProperty("auto.offset.reset",paramTool.get("kafka.auto.offset.reset"))
    properties.setProperty("enable.auto.commit",paramTool.get("kafka.enable.auto.commit"))
    properties.setProperty("group.id", paramTool.getRequired("kafka.group.id"))

    //保证flink source精准一次消费
    val topics= paramTool.getRequired("kafka.topics").split(",").toList
    import scala.collection.JavaConversions._
    val stream = env
      .addSource(new FlinkKafkaConsumer010[String](topics, new SimpleStringSchema(), properties)
        .setCommitOffsetsOnCheckpoints(false))
    stream
  }
}

object CreateKafkaSource{
  def main(args: Array[String]): Unit = {
    println(Time.of(10, TimeUnit.SECONDS).toMilliseconds)
  }

}
