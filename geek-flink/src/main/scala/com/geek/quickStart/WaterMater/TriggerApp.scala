package com.geek.quickStart.WaterMater

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows

object TriggerApp {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    triggerProcess(env)
    env.execute(this.getClass.getSimpleName)
  }


  def triggerProcess(env:StreamExecutionEnvironment): Unit ={

    val stream=env.socketTextStream("yqdata000",44444.toInt)
    val value= stream.map(x => {
      x.split(',')
    }).map(x=>{
      (x(0),x(1).toInt)
    }).keyBy(0)
      .timeWindow(Time.seconds(5))
      .trigger(new MyTrigger(3)).sum(1).print()

  }


}

class MyTrigger(maxNum:Int) extends Trigger[(String,Int),TimeWindow]{
  val reduceState= new ReducingStateDescriptor[Long]("reduce_state",new ReduceFunction[Long] {
    override def reduce(value1: Long, value2: Long): Long = {
      value1+value2
    }
  },classOf[Long])


  override def onElement(element: (String, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println("~~~~~~~~~~~~~onElement~~~~~~~~~~~~~~")
    ctx.registerProcessingTimeTimer(window.maxTimestamp)
    val stat=ctx.getPartitionedState(reduceState)
    stat.add(1)

    if(stat.get()>=maxNum) {
      stat.clear()
      println("~~~~~~~~~~~~~onElement FIRE_AND_PURGE~~~~~~~~~~~~~~")
      TriggerResult.FIRE_AND_PURGE
    }else{
      TriggerResult.CONTINUE
    }
  }


  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println("~~~~~~~~~~~~~onProcessingTime~~~~~~~~~~~~~~")
    TriggerResult.FIRE_AND_PURGE
  }


  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.getPartitionedState(reduceState).clear()
    ctx.deleteProcessingTimeTimer(window.maxTimestamp())
  }

  override def canMerge = true

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
}