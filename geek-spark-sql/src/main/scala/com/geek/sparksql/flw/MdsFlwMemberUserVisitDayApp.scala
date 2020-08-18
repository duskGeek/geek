package com.geek.sparksql.flw

import java.sql.{Connection, PreparedStatement}

import com.geek.sparksql.inter.AppLogging
import com.geek.utils.ContextUtils
import com.geek.utils.mysql.MysqlConnect
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object MdsFlwMemberUserVisitDayApp extends AppLogging {

  def main(args: Array[String]): Unit = {

    if (args == null || args.length <= 0) {
      logError("参数为空，请传入统计日期")

      return 1
    }
    val dt = args(0)

    val spark = ContextUtils.getSparkSession(this.getClass.getSimpleName, "yarn-client")



    val flwDf = spark.read.format("jdbc").
      option("driver", "com.mysql.jdbc.Driver").
      option("dbtable", " (SELECT time_local_date ,open_id FROM mocard_flw.rt_member_miniapp_flw_detail " +
        s"WHERE date(time_local_date)='$dt' ) flw ").load()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val aggRdd = flwDf.groupBy('time_local_date, 'open_id).
      agg(count('open_id).as("pv")).repartition(10)
    aggRdd.show()
    aggRdd.rdd.foreachPartition(partition=>{
      val userVisitList = new ListBuffer[Row]

      partition.foreach(row =>{

        userVisitList.append(row)
      })
      insertUserVisit(userVisitList)
      insertUserVisitNorbi(userVisitList)
    })
    spark.stop()
  }

  def insertUserVisit(list: ListBuffer[Row]): Unit = {
    var conn: Connection = null
    var pstm: PreparedStatement = null
    try {
      conn = MysqlConnect.getConnection()
      conn.setAutoCommit(false)
      val sql = "REPLACE INTO mds_flw_member_user_visit_day(record_dt,open_id,pv) values(?,?,?);"
      val pstm = conn.prepareStatement(sql)
      list.foreach(userVisit => {
        pstm.setDate(1, userVisit.getAs("time_local_date"))
        pstm.setString(2, userVisit.getAs("open_id"))
        pstm.setLong(3, userVisit.getAs("pv"))

        pstm.addBatch()
      })
      pstm.executeBatch()
      conn.commit()
    } finally {
      MysqlConnect.release(conn, pstm)
    }
  }

  def insertUserVisitNorbi(list: ListBuffer[Row]): Unit = {
    var conn: Connection = null
    var pstm: PreparedStatement = null
    try {
      conn = MysqlConnect.getConnectionNorbi()
      conn.setAutoCommit(false)
      val sql = "REPLACE INTO mds_flw_member_user_visit_day(record_dt,open_id,pv) values(?,?,?);"
      val pstm = conn.prepareStatement(sql)
      list.foreach(userVisit => {
        pstm.setDate(1, userVisit.getAs("time_local_date"))
        pstm.setString(2, userVisit.getAs("open_id"))
        pstm.setLong(3, userVisit.getAs("pv"))

        pstm.addBatch()
      })
      pstm.executeBatch()
      conn.commit()
    }
    finally{
      MysqlConnect.release(conn, pstm)
    }
  }
}
