package com.geek.dao.prewarning

import com.geek.utils.mysql.BaseDAO
import scalikejdbc._

import scala.collection.mutable.ListBuffer


class RiskWordDAO extends BaseDAO  {

  override def bacthInsert(list: ListBuffer[Seq[Any]]): Unit = {
    val sql= sql"REPLACE into risk_word (dt,word,count_num ) VALUES(?,?,?)";
    db.batchInsert(sql,list.toSeq)
  }




}
