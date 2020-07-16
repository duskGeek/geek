package com.geek.quickStart

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object MysqlSourceApp {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment




    env.execute(this.getClass.getSimpleName)

  }

}
