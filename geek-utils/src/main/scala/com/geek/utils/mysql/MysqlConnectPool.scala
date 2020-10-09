package com.geek.utils.mysql
import scalikejdbc.config._

object MysqlConnectPool {

  private var isInitYqdataPool=false
  def yqdataPoolSetup(): Unit ={
    if(!isInitYqdataPool){
      poolSetupAll("yqdata")
      isInitYqdataPool=true
    }
  }

  def yqdataPoolColse(): Unit ={
    poolColse("yqdata")
  }

  def poolSetupAll(prefix:String): Unit ={
    DBsWithEnv(prefix).setupAll()
  }

  def poolColse(prefix:String): Unit ={
    DBsWithEnv(prefix).closeAll()
  }

}
