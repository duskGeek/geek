package com.geek.dao.mysqlnorbi

import com.geek.utils.mysql.DBOperations

import scala.collection.mutable.ListBuffer

class FlwUserVisitDayDAO  {

  val db=DBOperations

  def bacthInsert(list:ListBuffer[Seq[Any]]): Unit ={
    val sql="REPLACE INTO flw_user_visit_day(record_dt,open_id,pv) values(?,?,?);";
    db.batchInsert(sql,list)
  }
}
