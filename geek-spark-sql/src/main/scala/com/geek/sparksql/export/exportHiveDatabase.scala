package com.geek.sparksql.export

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.geek.utils.ContextUtils

object exportHiveDatabase {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(ContextUtils.getConf(
      this.getClass.getSimpleName,"yarn-client")).
      enableHiveSupport().
      getOrCreate()

    val tableDs=spark.catalog.listTables("");

    val table=tableDs.rdd.map(x=>tableInfo(x.database,x.name,x.description)).collect()

    import spark.implicits._
    val infoRdd=table.map(table=>spark.catalog.listColumns(table.tableDB,table.tableName).
      map(col=>table.copy(colName = col.name,colType = col.dataType,colDesc = col.description)).
      rdd.collect())

    val flatRdd=spark.sparkContext.parallelize(infoRdd).flatMap(x=>x)

    flatRdd.toDS().map(_.toString).write.mode(SaveMode.Overwrite).
      format("text").save("//dw//tableinfo/")

  }

}

case class tableInfo(tableDB:String,tableName:String,tableDesc:String="",
                     tableLocation:String="",colName:String="",colType:String="",colDesc:String=""){
  override def toString: String = {
    tableDB+"\t"+tableName+"\t"+tableDesc+"\t"+
    tableLocation+"\t"+colName+"\t"+colType+"\t"+colDesc
  }
}