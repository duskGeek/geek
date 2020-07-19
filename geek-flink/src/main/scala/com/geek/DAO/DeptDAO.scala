package com.geek.DAO

import com.geek.quickStart.SourceFunctions.RiskWord
import com.geek.utils.mysql.BaseDAO
import scalikejdbc._

import scala.collection.mutable.ListBuffer


class DeptDAO extends BaseDAO  {

  override def bacthInsert(list: ListBuffer[Seq[Any]]): Unit = {
    val sql= sql"REPLACE into dept (dept_name , num ) VALUES(?,?)";
    db.batchInsert(sql,list.toSeq)
  }


}
