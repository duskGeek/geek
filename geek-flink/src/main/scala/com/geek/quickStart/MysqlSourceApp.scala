package com.geek.quickStart

import com.geek.quickStart.SourceFunctions.{MysqlFunction, RiskWord}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object MysqlSourceApp {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val riskWord=env.addSource[RiskWord](new MysqlFunction()).
      keyBy("word").
      timeWindow(Time.seconds(5)).
      sum("countNum")



    val splitWordStream=riskWord.split(x=>{
      if(x.word.contains("Exception")){
        List("error")
      }else{
        List("info")
      }
    })

    val outputTag = OutputTag[String]("side-output")

    splitWordStream.select("info").print("-----------------")
    splitWordStream.select("error").print("~~~~~~~")

    env.execute(this.getClass.getSimpleName)

  }

}
