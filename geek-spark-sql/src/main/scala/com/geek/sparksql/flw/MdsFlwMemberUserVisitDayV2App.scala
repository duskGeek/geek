package com.geek.sparksql.flw

import com.geek.common.SparkReadCommon
import com.geek.dao.mysqlnorbi.MdsFlwMemberUserVisitDayDAO
import com.geek.sparksql.inter.AppLogging
import com.geek.utils.ContextUtils
import com.geek.utils.mysql.MysqlConnectPool

import scala.collection.mutable.ListBuffer

object MdsFlwMemberUserVisitDayV2App extends AppLogging {

  def main(args: Array[String]): Unit = {
    if (args == null || args.length <= 0) {
      logError("参数为空，请传入统计日期")
      return 1
    }
    val dt = args(0)

    val spark = ContextUtils.getSparkSession(this.getClass.getSimpleName, "yarn-client")

    val flwDf = SparkReadCommon.yctidbDFReader(spark).
      option("dbtable", " (SELECT time_local_date ,open_id FROM mocard_flw.rt_member_miniapp_flw_detail " +
        s"WHERE date(time_local_date)='$dt' ) flw ").load()

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val aggRdd = flwDf.groupBy('time_local_date, 'open_id).
      agg(count('open_id).as("pv")).repartition(10)

//    MysqlConnectPool.yctidbPoolSetup()

    try{
      aggRdd.rdd.foreachPartition(partition=>{
        val userVisitList = new ListBuffer[Seq[Any]]

        partition.foreach(row =>{
          val userVisit= Seq(row.getAs("time_local_date"),
            row.getAs("open_id"),
            row.getAs("pv"))
          userVisitList.append(userVisit)
        })
        val userVisitDao= new MdsFlwMemberUserVisitDayDAO
        userVisitDao.bacthInsert(userVisitList)
        //val userVisitNorbiDao=new MdsFlwMemberUserVisitDayDAO
        //userVisitNorbiDao.bacthInsert(userVisitList)
      })
    }finally {
      spark.stop()
//      MysqlConnectPool.yctidbPoolColse()
    }
  }

}
