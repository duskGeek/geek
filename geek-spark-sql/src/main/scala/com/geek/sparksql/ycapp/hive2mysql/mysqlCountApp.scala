package com.geek.sparksql.ycapp.hive2mysql

import java.util.Properties

import com.geek.utils.ContextUtils

object mysqlCountApp {

  def main(args: Array[String]): Unit = {

    val spark = ContextUtils.getSparkSession(this.getClass.getSimpleName, "")

    val connectionProperties=new  Properties()

    //spark.read.format("jdbc").option("",connectionProperties)






    spark.stop()
  }


}
