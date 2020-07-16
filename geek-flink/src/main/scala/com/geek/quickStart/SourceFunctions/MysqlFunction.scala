package com.geek.quickStart.SourceFunctions

import java.util.Date

import com.geek.utils.mysql.MysqlConnectPool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class MysqlFunction extends RichSourceFunction[RiskWord]{
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    MysqlConnectPool.yqdataPoolSetup()
  }

  override def run(ctx: SourceFunction.SourceContext[RiskWord]): Unit = {

  }


  override def close(): Unit = {
    MysqlConnectPool.yqdataPoolColse()
  }
  override def cancel(): Unit = {

  }
}
case class RiskWord(id:Int,dt:Date,word:String,countNum:Long)