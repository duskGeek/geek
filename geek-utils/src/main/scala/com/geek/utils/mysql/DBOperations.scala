package com.geek.utils.mysql

import scalikejdbc._

import scala.collection.mutable.ListBuffer

/**
  * 提供数据库插入、查询、更新、删除等公共操作
  */
object DBOperations extends Serializable {

  def batchInsert( sqlStr:String,params:ListBuffer[Seq[Any]] ): Unit ={
    DB localTx { implicit session =>
        sql"$sqlStr".batch(params: _*).apply()
    }
  }

  def batchInsert( sql:SQL[Nothing, NoExtractor],params:Seq[Seq[Any]] ): Unit ={
    DB localTx { implicit session =>
      sql.batch(params: _*).apply()
    }
  }

    def batchByNameInsert( sql:SQL[Nothing, NoExtractor],params:Seq[Seq[(Symbol, Any)]] ): Unit ={
      DB localTx { implicit session =>
        sql.batchByName(params: _*).apply()
      }
  }
}
