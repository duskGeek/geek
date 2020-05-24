package com.geek.utils.mysql

import java.sql.{Connection, DriverManager, PreparedStatement}

object MysqlConnect {

  private var connection: Connection = _

  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://yqdata000:3306/yqdata?useUnicode=true&characterEncoding=utf-8&useSSL=false"
  val username = "root"
  val password = "yq1234!@#$"

   def conn(): Connection = {

    if (connection == null) {
      println(this.driver)
      Class.forName(this.driver)
      connection = DriverManager.getConnection(this.url, this.username, this.password)
    }
    connection
  }

  def getConnection() : Connection = {
    Class.forName(this.driver)
    DriverManager.getConnection(this.url, this.username, this.password)
  }

  def getConnectionNorbi() : Connection = {
    Class.forName(this.driver)
    val url = "jdbc:mysql://mysqlnorbi.service.ycidcm5:3306/member?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    val username = "member_rw"
    val password = "w@YL1E7@Y6dn0ugA"
    DriverManager.getConnection(url, username, password)
  }

  def close(): Unit ={
    if(connection!=null)
      connection.close()
  }

  def release(conn:Connection, pstm:PreparedStatement): Unit ={
    if(conn!=null){
      conn.close()
    }
    if(pstm!=null){
      pstm.close()
    }
  }


}
