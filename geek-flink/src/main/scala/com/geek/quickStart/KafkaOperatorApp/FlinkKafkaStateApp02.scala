package com.geek.quickStart.KafkaOperatorApp

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkKafkaStateApp02 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val paramterTool=ParameterTool.fromPropertiesFile("conf/flink-job.properties")
    val kafkaStream=new CreateKafkaSource(env).create(paramterTool)

    kafkaStream.
      assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](
        Time.seconds(0)) {
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
      window(TumblingEventTimeWindows.of(Time.seconds(5))).
      sum(1)
      .print()

    env.execute(this.getClass.getSimpleName)
  }
}
