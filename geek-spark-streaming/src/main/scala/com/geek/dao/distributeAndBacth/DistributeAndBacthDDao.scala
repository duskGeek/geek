package com.geek.dao.distributeAndBacth

import com.geek.sparkstreaming.distributeAndBacth.orderInfo
import com.geek.utils.mysql.BaseDAO
import scalikejdbc._

import scala.collection.mutable.ListBuffer

class DistributeAndBacthDDao[A] extends BaseDAO{

  override def bacthInsert(list: ListBuffer[Seq[Any]]): Unit = {
    val sql= sql"REPLACE INTO order_info(order_id,money,catagory) values(?,?,?)";
    db.batchInsert(sql,list.toSeq)
  }

  def bacthInsert(list: ListBuffer[Seq[Any]],tableName:String): Unit = {
    var orderTable=new OrderTable(tableName)
      DB localTx { implicit session =>
        list.foreach(seq=>{
          val orderId=seq(0)
          val money=seq(1)
          val catagory=seq(2)
          sql"REPLACE INTO  ${orderTable.table}(order_id,money,catagory) values(${orderId},${money},${catagory})"
            .update.apply()
        })
      }
  }

  def bacthInsert(map: Map[String,ListBuffer[Seq[Any]]]): Unit = {
    if (map ==null||map.isEmpty){
      return
    }
    map.foreach(entry=>{
      bacthInsert(entry._2,entry._1)
    })
  }
}

class OrderTable(val mytableName: String) extends SQLSyntaxSupport[orderInfo]  {
  override val tableName = s"$mytableName"

  def apply(o: ResultName[orderInfo])(rs: WrappedResultSet) = new orderInfo(
    orderId  = rs.string(o.orderId),
    money = rs.bigDecimal(o.money),
    catagory  = rs.int(o.catagory)
  )
}