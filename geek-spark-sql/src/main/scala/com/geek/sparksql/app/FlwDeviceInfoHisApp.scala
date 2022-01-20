package com.geek.sparksql.app

import com.geek.utils.ContextUtils
import org.apache.spark.sql.{Column, SparkSession}
import org.elasticsearch.spark.sql._

//PUT /fact_app_flw_device_info_his/_doc
//{
//"settings": {
//"number_of_shards" :   1,
//"number_of_replicas" : 0
//},
//"mappings": {
//"dev":{
//"properties": {
//"device_id": {
//"type": "text"
//},
//"new_cid": {
//"type": "text"
//}
//}
//}
//}
//}

object FlwDeviceInfoHisApp {

  def main(args: Array[String]): Unit = {
    val spark = ContextUtils.getSparkSessionESSupport(this.getClass.getSimpleName, "local[2]")
    writeEs(spark)
    //readFormat(spark)
    spark.stop()
  }

  def readFormat(spark: SparkSession): Unit ={
    val options = Map("pushdown" -> "true",
      "es.nodes" -> "",
      "es.port" -> "9200")
    val df = spark.read.format("es").options(options)
      .load("flw_device_info_his_orc")
    df.printSchema()
    df.show

    import spark.implicits._
    import org.apache.spark.sql.functions._
    df.select('device_id).show
    df.select(count('device_id)).show
  }

  def writeEs(spark: SparkSession): Unit ={
     val df=spark.read.orc("./inputDir/device_info_new_his")
    import spark.implicits._
    import org.apache.spark.sql.functions.lit
    val littleDf=df.select('device_id,'new_cid,'first_visit_dt).
      withColumn("last_visit_dt",lit("2")).withColumn("id",'device_id)

    EsSparkSQL.saveToEs(littleDf,"device_info_his_orc/data",Map("es.mapping.id" -> "id"))

  }

}
