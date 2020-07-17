package com.geek.quickStart.SourceFunctions

import java.util.Date

import com.geek.DAO.RiskWordDAO
import com.geek.utils.mysql.MysqlConnectPool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class MysqlFunction extends RichSourceFunction[RiskWord]{

  var riskWordDAO :RiskWordDAO= _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    MysqlConnectPool.yqdataPoolSetup()
    riskWordDAO=new RiskWordDAO()
  }

  override def run(ctx: SourceFunction.SourceContext[RiskWord]): Unit = {
    val riskWordList=riskWordDAO.queryAll()
    riskWordList.foreach(x=>{
      ctx.collect(x)
      Thread.sleep(1000)
    })
  }


  override def close(): Unit = {
    MysqlConnectPool.yqdataPoolColse()
  }
  override def cancel(): Unit = {

  }
}
case class RiskWord(id:Int,dt:Date,word:String,countNum:Long)