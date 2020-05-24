package com.geek.utils.mysql

import scala.collection.mutable.ListBuffer
import scalikejdbc._

trait BaseDAO extends Serializable {

  val db=DBOperations

  def bacthInsert(list:ListBuffer[Seq[Any]])

  def bacthInsertByName(list: ListBuffer[Seq[(Symbol, Any)]]): Unit = {
    val sqlStr=sql""
    db.batchByNameInsert(sqlStr,list.toSeq)
  }
}
