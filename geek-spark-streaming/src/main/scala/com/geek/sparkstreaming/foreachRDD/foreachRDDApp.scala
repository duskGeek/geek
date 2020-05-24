package com.geek.sparkstreaming.foreachRDD

import com.geek.dao.foreachRDD.ForeachRddDao
import com.geek.utils.ContextUtils
import com.geek.utils.mysql.MysqlConnectPool
import org.apache.spark.streaming.Seconds

import scala.collection.mutable.ListBuffer


object foreachRDDApp {

  val slide_interval=Seconds(5)

  def main(args: Array[String]): Unit = {

    val ssc=ContextUtils.getSparkStreamContext(this.getClass.getSimpleName,"local[2]",slide_interval)

    val sockTextStream=ssc.socketTextStream("yqdata000",44444)

    val resultStream=sockTextStream.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)

    MysqlConnectPool.yqdataPoolSetup()

    val foreachRddDao=new ForeachRddDao()

    resultStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iterator=>{
        //val buffer = new ListBuffer[Seq[(Symbol, Any)]]()
        val buffer = new ListBuffer[Seq[Any]]()
        var seq=null
        iterator.foreach(x=>{
          buffer+=Seq(x._1,x._2)
          //buffer+=Seq('dept_name -> x._1,'num -> x._2)
        })
        if(buffer.size>0){
          //foreachRddDao.bacthInsertByName(buffer)
          foreachRddDao.bacthInsert(buffer)
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
    MysqlConnectPool.yqdataPoolColse()
  }

}
