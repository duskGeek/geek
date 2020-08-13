package com.geek.quickStart.tableapi

import com.geek.quickStart.SourceFunctions.Access
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._

object TableStreamApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //normalStream(env)
    updateStream(env)
    env.execute(this.getClass.getSimpleName)
  }

  def normalStream(env:StreamExecutionEnvironment): Unit ={
    val tableEnv=StreamTableEnvironment.create(env)

    val dataStream=env.socketTextStream("yqdata000",44444).map(x=>{
      val array=x.split(",")
      Access(array(0),array(1).toLong,array(2).toInt)
    })

    val table=tableEnv.fromDataStream[Access](dataStream)
    tableEnv.createTemporaryView("access",table)
    //val resultTable=tableEnv.sqlQuery("select `time`,domain,traffic from access")

    val resultTable=table.select('time,'domain,'traffic)

    resultTable.printSchema()
    tableEnv.toAppendStream[Row](resultTable).print()
  }

  def updateStream(env:StreamExecutionEnvironment) ={

    val tableEnv=StreamTableEnvironment.create(env)

    val dataStream=env.socketTextStream("yqdata000",44444).map(x=>{
      val array=x.split(",")
      Access(array(0),array(1).toLong,array(2).toInt)
    })
    val table=tableEnv.fromDataStream[Access](dataStream)
    tableEnv.createTemporaryView("access",table)

    //写法一
   // val resultTable=tableEnv.sqlQuery("select domain,sum(traffic) from access group by domain")

    //写法二
//    table.groupBy('domain).
//      select('domain,'traffic.sum).
//      toRetractStream[Row].filter(_._1).print()

   //写法三
   val resultTable=tableEnv.sqlQuery(
      s"""
        |select
        |domain,sum(traffic)
        |from $table
        |group by domain
      """.stripMargin)

    resultTable.printSchema()
    tableEnv.toRetractStream[Row](resultTable).print()

  }
}

