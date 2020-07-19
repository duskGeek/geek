package com.geek.quickStart

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedStateApp {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //valueStateFun(env)
    //mapState(env)
    mapState2(env)
    env.execute(this.getClass.getSimpleName)
  }

  def mapState(env: StreamExecutionEnvironment): Unit = {
    env.fromCollection(List(
      (1L, 2L),
      (1L, 4L),
      (1L, 6L),
      (2L, 3L),
      (1L, 5L),
      (2L, 6L),
      (1L, 3L),
      (2L, 2L),
      (2L, 2L),
      (1L, 3L)
    )).keyBy(0).flatMap(new WindowAverageMapState).print()
  }

    def mapState2(env: StreamExecutionEnvironment): Unit = {
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
      )).keyBy(0).flatMap(new WindowAverageMapState2).print()
    }


    def valueStateFun(env: StreamExecutionEnvironment): Unit = {
      env.fromCollection(List(
        (1L, 2L),
        (1L, 4L),
        (1L, 6L),
        (2L, 3L),
        (1L, 5L),
        (2L, 6L),
        (1L, 3L),
        (2L, 2L),
        (2L, 2L),
        (1L, 3L)
      )).keyBy(0).flatMap(new WindowAverage).print()
    }

}


class WindowAverageMapState2 extends RichFlatMapFunction[(Long,String),(Long,String,Long)]{

  var sum:MapState[String,Long] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    sum=getRuntimeContext.getMapState(
      new MapStateDescriptor[String,Long](
        "mapStateDesc",TypeInformation.of(new TypeHint[String] {}),
        TypeInformation.of(new TypeHint[Long] {})))
  }

  override def flatMap(value: (Long,String), out: Collector[(Long,String,Long)]): Unit = {
    val oldState=sum.get(value._2)

    val currState=if(oldState!=null){
      oldState+1
    }else{
      0
    }
    sum.put(value._2,currState)

    out.collect(value._1,value._2,currState)
  }
}

class WindowAverageMapState extends RichFlatMapFunction[(Long,Long),(Long,Long)]{

  var sum:MapState[Long,(Long,Long)] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    sum=getRuntimeContext.getMapState(
      new MapStateDescriptor[Long,(Long,Long)](
        "mapStateDesc",TypeInformation.of(new TypeHint[Long] {}),
        TypeInformation.of(new TypeHint[(Long,Long)] {})))
  }

  override def flatMap(value: (Long, Long), out: Collector[(Long, Long)]): Unit = {

    val oldState=sum.get(value._1)

    val curState=if(oldState!=null){
      (oldState._1+1,oldState._2+value._2)
    }else {
      (1L,value._2)
    }

    sum.put(value._1,curState)
    if(curState._1==2){
      out.collect(value._1,curState._2/curState._1)
      sum.remove(value._1)
    }

  }
}

class WindowAverage extends RichFlatMapFunction[(Long,Long),(Long,Long)]{

  var sum:ValueState[(Long,Long)] =_

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    sum=getRuntimeContext.getState(
      new ValueStateDescriptor[(Long,Long)](
        "valueStateDesc",classOf[(Long,Long)]))
  }

  override def flatMap(value: (Long, Long), out: Collector[(Long, Long)]): Unit = {

    var tmpStat=if(sum.value()!=null){
      sum.value()
    }else {
      (0L,0L)
    }
    val newsum=(tmpStat._1+1,tmpStat._2+value._2)
    sum.update(newsum)

    if(newsum._1==2){
      out.collect(value._1,newsum._2/newsum._1)
      sum.clear()
    }
  }
}
