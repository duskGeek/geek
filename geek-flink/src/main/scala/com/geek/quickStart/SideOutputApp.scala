package com.geek.quickStart

import com.geek.quickStart.SourceFunctions.{MysqlFunction, RiskWord}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val outputTag = OutputTag[RiskWord]("side-output")

    val riskWordStream=env.addSource[RiskWord](new MysqlFunction()).
      process(new ProcessFunction[RiskWord, RiskWord] {
      override def processElement(value: RiskWord,
                                  ctx: ProcessFunction[RiskWord,RiskWord]#Context,
                                  out: Collector[RiskWord]): Unit = {
        if(value.word.contains("Exception")){
          ctx.output(outputTag,value)
        }else{
          out.collect(value)
        }
      }
    })

      riskWordStream.keyBy("word").
      timeWindow(Time.seconds(5)).
      sum("countNum").print("-------")

    riskWordStream.getSideOutput(outputTag).print("~~~~~~~~~~~")

    env.execute(this.getClass.getSimpleName)
  }

}
