package com.geek.dao.xxxoffsetmamager

import com.geek.utils.mysql.BaseDAO
import scalikejdbc._

import scala.collection.mutable.ListBuffer


class XxxOffsetManagerDAO extends BaseDAO  {

  override def bacthInsert(list: ListBuffer[Seq[Any]]): Unit = {
    val sql= sql"REPLACE INTO dept(dept_name,num) values(?,?)";
    db.batchInsert(sql,list.toSeq)
  }

}
