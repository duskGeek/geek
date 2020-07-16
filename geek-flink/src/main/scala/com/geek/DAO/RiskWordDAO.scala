package com.geek.DAO

import com.geek.quickStart.SourceFunctions.RiskWord
import com.geek.utils.mysql.BaseDAO
import scalikejdbc._

import scala.collection.mutable.ListBuffer


class RiskWordDAO extends BaseDAO  {

  override def bacthInsert(list: ListBuffer[Seq[Any]]): Unit = {
    val sql= sql"REPLACE into risk_word (dt,word,count_num ) VALUES(?,?,?)";
    db.batchInsert(sql,list.toSeq)
  }

  def queryAll(): List[RiskWord] ={
    val sql = sql"select id,dt,word,count_num from risk_word"
    db.queryAll(sql,
      rs=>{
        RiskWord(rs.int("id"),
          rs.date("dt"),
          rs.string("word"),
          rs.long("count_num"))
      }
    )
  }

}
