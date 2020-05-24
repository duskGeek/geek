package com.geek.utils.mysql
import scalikejdbc.config._

object MysqlConnectPool {
  def yqdataPoolSetup(): Unit ={
    poolSetupAll("yqdata")
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
