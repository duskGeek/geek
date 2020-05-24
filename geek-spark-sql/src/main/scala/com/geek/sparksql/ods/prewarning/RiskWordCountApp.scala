package com.geek.sparksql.ods.prewarning

import com.geek.dao.prewarning.RiskWordDAO
import com.geek.sparksql.inter.AppLogging
import com.geek.sparksql.ods.prewarning.PrewarningETLApp.logError
import com.geek.utils.ContextUtils
import com.geek.utils.ContextUtils.getConf
import com.geek.utils.mysql.MysqlConnectPool
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object RiskWordCountApp  extends AppLogging{

  def main(args: Array[String]): Unit = {
      if (args == null || args.length <= 0) {
        logError("参数为空，请传入统计日期")

        return 1
      }

    val dt=args(0)
    val spark = ContextUtils.getSparkSession(this.getClass.getSimpleName, "")
    //val spark= SparkSession.builder().config(ContextUtils.getConf(this.getClass.getSimpleName,
     // "yarn-client")).enableHiveSupport().getOrCreate()

    val sqlContext=spark.sqlContext

    val loginfoDF=sqlContext.sql(s"select loginfo from default.prewarning where dt='$dt' and logtype<>'INFO' ")

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val wordcount=loginfoDF.select(explode(split('loginfo," ")).
      as("word")).
      groupBy('word).
      count()

      wordcount.rdd.foreachPartition(partition=>{
        try{
             MysqlConnectPool.yqdataPoolSetup()
             val riskWordDAO=new RiskWordDAO()
             val words=new ListBuffer[Seq[Any]]()
             partition.foreach(wordCount=>{
               words.append(Seq(dt,wordCount.getString(0),wordCount.getLong(1)))
             })
             riskWordDAO.bacthInsert(words)
        }finally{
          MysqlConnectPool.yqdataPoolColse()
        }
      })

    spark.stop()

  }
}
