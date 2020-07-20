package com.geek.quickStart

import java.util
import java.util.Collections

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object OperatorStateApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    listOperatorState(env)
    env.execute(this.getClass.getSimpleName)
  }

  def listOperatorState(env: StreamExecutionEnvironment): Unit ={
    env.fromCollection(List(
      (1L, "buy"),
      (1L, "add"),
      (1L, "fav"),
      (1L, "fav"),
      (1L, "buy"),
      (1L, "buy"),
      (2L, "add"),
      (2L, "buy"),
      (2L, "buy"),
      (2L, "buy"),
    )).keyBy(0).flatMap(new CountWindowListState()).print()

  }

}

class CountWindowListState extends RichFlatMapFunction[(Long,String),(Long,String,Long)] with ListCheckpointed[Integer]{

  var count:Integer=0

  override def flatMap(value: (Long, String), out: Collector[(Long,String, Long)]): Unit = {
    if(value._2=="buy"){
      count+=1
      out.collect(value._1,value._2,count.toLong)
    }

  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Integer] = {
    Collections.singletonList(count)
  }

  override def restoreState(state: util.List[Integer]): Unit = {
    import scala.collection.JavaConversions._
    for( ele <- state){
      count=ele
    }
  }
}