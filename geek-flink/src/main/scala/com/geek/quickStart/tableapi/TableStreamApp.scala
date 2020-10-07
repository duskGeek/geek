package com.geek.quickStart.tableapi

import com.geek.quickStart.SourceFunctions.Access
import com.geek.utils.parse.GetIPAddress
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Tumble}
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction, TableFunction}



object TableStreamApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //normalStream(env)
    //updateStream(env)
    //udfGetIpStream(env)
    //udtfExplodStream(env)
    //tumblWindow(env)
    //slideWindow(env)
    overWindowFun(env)
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

  def udfGetIpStream (env:StreamExecutionEnvironment){
    val tableEnv=StreamTableEnvironment.create(env)

    val dataStream=env.socketTextStream("yqdata000",44444).map(x=>{
      val array=x.split(",")
      Access2(array(0),array(1).toLong,array(2).toInt,array(3))
    })

    val table =tableEnv.fromDataStream[Access2](dataStream)
    tableEnv.registerFunction("parseIp",new GetLocation)
    tableEnv.createTemporaryView("access2",table)

    val parseIpUdf=new GetLocation
    table.select(parseIpUdf('ip).as('ip)).toAppendStream[Row].print()

  }

  def udtfExplodStream (env:StreamExecutionEnvironment){
    val tableEnv=StreamTableEnvironment.create(env)

    val dataStream=env.socketTextStream("yqdata000",44444).map(x=>{
      val array=x.split(",")
      Access2(array(0),array(1).toLong,array(2).toInt,array(3))
    })

    val table =tableEnv.fromDataStream[Access2](dataStream)
    tableEnv.registerFunction("split",new SplitUDTF)
    tableEnv.createTemporaryView("access2",table)
    val splitUDTF=new SplitUDTF

    tableEnv.sqlQuery("select domain,`time`,traffic,key,content " +
      //left join 在udtf没有输出任何数据的情况下，当前数据仍然会输出，但是key和content输出为null
      //"from access2 left join lateral table (split(ip)) as t(key,content) on true").
      //不用left join 相当于CROSS JOIN ，内连接，如果UDTF什么也不输出，那么整条记录也不会输出
      "from access2 , lateral table (split(ip)) as t(key,content) on true").
      toAppendStream[Row].print()

  }

  def tumblWindow(env:StreamExecutionEnvironment): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val dataStream = env.socketTextStream("yqdata000", 44444).filter(_!=null).map(x => {
      val array = x.split(",")
      Access2(array(0), array(1).toLong, array(2).toInt, array(3))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Access2](Time.seconds(0)) {
      override def extractTimestamp(t: Access2): Long = {
        t.time
      }
    })
    println(dataStream.getParallelism)
    val tableEnv = StreamTableEnvironment.create(env)

    tableEnv.createTemporaryView("access2", dataStream, 'domain,  'traffic, 'ip, 'rowtime.rowtime)
//    val table = tableEnv.from("access2").
//      window(Tumble.over(5.second).on('rowtime).as("win"))
//      .groupBy('domain, 'win)
//      .select('domain, 'win.start, 'win.end,'win.rowtime, 'traffic.sum)

    val table=tableEnv.sqlQuery(
      """
        |select domain,
        |TUMBLE_START(rowtime,interval '5' second),
        |TUMBLE_END(rowtime,interval '5' second),
        |TUMBLE_ROWTIME(rowtime,interval '5' second),
        |SUM(traffic) AS  traffic
        |FROM access2
        |GROUP BY domain,TUMBLE(rowtime,interval '5' second)
      """.stripMargin)

   tableEnv.toRetractStream[Row](table).print()
  }
  def slideWindow(env:StreamExecutionEnvironment): Unit = {

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream = env.socketTextStream("yqdata000", 44444).filter(_!=null).map(x => {
      val array = x.split(",")
      Access2(array(0), array(1).toLong, array(2).toInt, array(3))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Access2](Time.seconds(0)) {
      override def extractTimestamp(t: Access2): Long = {
        t.time
      }
    })

    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.createTemporaryView("access2", dataStream, 'domain,  'traffic, 'ip, 'rowtime.rowtime)

    val table= tableEnv.from("access2").
      window(Slide.over("4.seconds").every("2.seconds").on('rowtime).as("win")).
      groupBy('domain,'win).select('domain,'win.end,'win.start,'traffic.sum)

    tableEnv.toRetractStream[Row](table).print()
  }


  def overWindowFun(env:StreamExecutionEnvironment): Unit ={

    val envSetting=EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream = env.socketTextStream("yqdata000", 44444).filter(_!=null).map(x => {
      val array = x.split(",")
      Access2(array(0), array(1).toLong, array(2).toInt, array(3))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Access2](Time.seconds(0)) {
      override def extractTimestamp(t: Access2): Long = {
        t.time
      }
    })
    val tableEnv = StreamTableEnvironment.create(env,envSetting)
    tableEnv.createTemporaryView("access2", dataStream, 'domain,'time, 'traffic, 'ip,'proctime.proctime)

    val table=tableEnv.sqlQuery(
      """
        |SELECT
        |domain,`time`,traffic,ip,
        |ROW_NUMBER() OVER (PARTITION BY domain,ip ORDER BY proctime ASC ) AS row_num
        |FROM access2
      """.stripMargin)

    table.printSchema()
    tableEnv.toRetractStream[Row](table)
  }


}

case class Access2(domain:String,time:Long,traffic:Long,ip:String)

class GetLocation extends ScalarFunction {
  override def open(context: FunctionContext): Unit = {
    println("udf :excute open ")
  }

  def eval(ip:String) :String={
    val location=GetIPAddress.search(ip)
    location
  }

}


class SplitUDTF extends TableFunction[(String,Int)]{
  def eval(str:String) ={
    val array=str.split("#")
    array.foreach(x=>{
      val kv=x.split(":")
      if(kv.size>1)
        collect(kv(0),kv(1).toInt)
    })
  }

}