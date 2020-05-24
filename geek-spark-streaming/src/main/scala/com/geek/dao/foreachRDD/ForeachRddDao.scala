package com.geek.dao.foreachRDD

import com.geek.utils.mysql.BaseDAO
import scalikejdbc._
import scala.collection.mutable.ListBuffer


class ForeachRddDao extends BaseDAO  {

  override def bacthInsert(list: ListBuffer[Seq[Any]]): Unit = {
    val sql= sql"REPLACE INTO dept(dept_name,num) values(?,?)";
    db.batchInsert(sql,list.toSeq)
  }

   override def bacthInsertByName(list: ListBuffer[Seq[(Symbol, Any)]]): Unit = {
   val sqlStr=sql"REPLACE INTO dept(dept_name,num) values({dept_name},{num})"
    db.batchByNameInsert(sqlStr,list.toSeq)
  }
}
