package com.geek.common

import com.geek.contanst.SparkReadContanst
import org.apache.spark.sql.{DataFrameReader, SparkSession}

object SparkReadCommon {

  def jdbcRead(spark: SparkSession): DataFrameReader ={
    spark.read.format("jdbc")
  }

  def yctidbDFReader(spark: SparkSession): DataFrameReader ={
    jdbcRead(spark).option("user", SparkReadContanst.YC_TIDB_READ_USER).
      option("password", SparkReadContanst.YC_TIDB_READ_PASSWORD).
      option("url", SparkReadContanst.YC_TIDB_READ_URL).
      option("driver", SparkReadContanst.MYSQL_DRIVER_CLASS_NAME)
  }


}
