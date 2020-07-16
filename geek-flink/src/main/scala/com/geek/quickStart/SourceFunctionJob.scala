package com.geek.quickStart

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SourceFunctionJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment





    env.execute(this.getClass.getSimpleName)
  }

}
