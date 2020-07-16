package com.geek.quickStart.SourceFunctions

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class ListOutFunction extends RichParallelSourceFunction[Access] {

  val running=true

  val domains=Array("www.baidu.com","www.taobao.com","www.alibaba.com")

  val random= new Random()

  override def run(sourceContext: SourceFunction.SourceContext[Access]): Unit = {
    while (running){
      1.to(10).foreach(x=>{
        val time=System.currentTimeMillis()
        val access=Access(domains(random.nextInt(3)),time,(x*random.nextInt(100)))
        sourceContext.collect(access)
      })
      Thread.sleep(5000)
    }
  }

  override def cancel(): Unit = {
  }
}


case class Access(domain:String,time:Long,traffic:Long)


