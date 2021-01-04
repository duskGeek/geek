package com.geek.DAO

import com.geek.utils.mysql.DBOperations
import com.geek.utils.mysql.settings.YqdataDBSetting
import scalikejdbc.DB
import scalikejdbc._

class CatagoryDAO extends YqdataDBSetting{

  val db=DBOperations

  def findById(catagoryId:Int) : String ={
    val sql=sql"select catagory_name from catagory where catagory_id=${catagoryId}"
    DB readOnly { implicit session=>
      sql.map(rs=>{rs.string("catagory_name")}).single().apply().getOrElse("")
    }
  }

}
