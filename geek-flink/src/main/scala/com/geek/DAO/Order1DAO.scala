package com.geek.DAO

import com.geek.utils.mysql.DBOperations
import com.geek.utils.mysql.settings.YqdataDBSetting
import scalikejdbc.DB
import scalikejdbc._

class Order1DAO  extends YqdataDBSetting{

  val db=DBOperations

  def findById(orderId:Int) : String ={
    val sql=sql"select order_id from order1 where order_id=${orderId}"
    DB readOnly { implicit session=>
      sql.map(rs=>{rs.string("order_id")}).single().apply().getOrElse("")
    }
  }


}
