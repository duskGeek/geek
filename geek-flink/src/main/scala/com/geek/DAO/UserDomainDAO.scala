package com.geek.DAO

import scalikejdbc.DB
import scalikejdbc._

class UserDomainDAO {

  def findUserIdByDomain(domain:String): Option[Int] ={
    DB readOnly { implicit session=>
      sql"select user_id from user_domain where domain=${domain}".map(rs=>rs.int("user_id")).single().apply()
    }
  }

}
