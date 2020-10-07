package com.geek.quickStart.hbase

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Mutation, Put, Result, Scan}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.util.Bytes
import org.apache.flink.api.java.tuple._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

object HBaseReadApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //writeHbase(env)
    readHbase(env)
    env.execute(this.getClass.getSimpleName)
  }

  def writeHbase(env:ExecutionEnvironment): Unit ={

    val list=new ListBuffer[(String,String,Int,String)]()

    for(i<- 1 to 10){
      list.append((i+"","yqy"+i,30,i+""))
    }
    val dataSet=env.fromCollection(list)

    val result=dataSet.map(new RichMapFunction[(String,String,Int,String),(Text,Mutation)] {
      val familyColumn="o".getBytes

      override def map(value: (String, String, Int, String)): (Text, Mutation) = {
       val id=new Text(value._1)
        val put=new Put(value._1.getBytes)
        put.addColumn(familyColumn,"name".getBytes,value._2.getBytes)
        put.addColumn(familyColumn,"age".getBytes,value._3.toString.getBytes())
        put.addColumn(familyColumn,"context".getBytes,value._4.getBytes)
        (id,put)
      }
    })

    val configuration=HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum","yqdata000")
    configuration.set("hbase.zookeeper.property.clientPort","2181")
    //设置表名
    configuration.set(TableOutputFormat.OUTPUT_TABLE,"sparkext:yq_status")
    configuration.set("mapreduce.output.fileoutputformat.outputdir","/tmp")
    val job=Job.getInstance(configuration)

    result.output(new HadoopOutputFormat[Text,Mutation](new TableOutputFormat[Text],job))
  }


  def readHbase(env:ExecutionEnvironment): Unit ={
    val familyColumn="o".getBytes

    val ds= env.createInput(new TableInputFormat[Tuple4[String,String,Int,String]] {
      def createTable(): HTable ={
        val configuration=HBaseConfiguration.create()
        configuration.set("hbase.zookeeper.quorum","yqdata000")
        configuration.set("hbase.zookeeper.property.clientPort","2181")

        new HTable(configuration,getTableName)
      }


       override def configure(parameters: Configuration): Unit = {
         this.table = createTable
         if (this.table != null) this.scan = this.getScanner
       }

       override def getScanner: Scan = {
         val scan=new Scan()
         scan.addFamily(familyColumn)
         scan
       }

       override def getTableName: String = "sparkext:yq_status"

       override def mapResultToTuple(result: Result): Tuple4[String, String, Int, String] = {
        new Tuple4(Bytes.toString(result.getRow) ,Bytes.toString(result.getValue(familyColumn,"name".getBytes)),
           Bytes.toString(result.getValue(familyColumn,"age".getBytes)).toInt,
           Bytes.toString(result.getValue(familyColumn,"context".getBytes)))
       }

     })
    ds.print()
  }

}
