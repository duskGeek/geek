package com.geek.quickStart

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object StreamJob {

  def main(args: Array[String]): Unit = {
    val param=ParameterTool.fromArgs(args)

    val port=param.getRequired("port")
    val host=param.getRequired("host")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream=env.socketTextStream(host,port.toInt)
    val value: KeyedStream[Wc, Tuple] = stream.flatMap(x => {
      x.split(',')
    }).map(x => {
      Wc(x,1)
    }).
      keyBy("text")

    value.
      timeWindow(Time.seconds(5)).
      sum("num").print()

    env.execute(this.getClass.getSimpleName)
  }

  case class Wc(text:String,num:Int)
}
