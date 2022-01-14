package com.geek.sparksql.cdnAccess

import com.geek.utils.mysql.MysqlConnect
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

object AccessLogDriver {
  def main(args: Array[String]): Unit = {

    if(args==null||args.length<2){
      println("请输入参数，输入文件地址和输出文件地址")
      return
    }


//    val out="out"
//    val in="inputDir/accesslog.txt"
//    val conf=new SparkConf().setMaster("local[2]").setAppName("access")
//    .set("spark.extraListeners","MyListenter")

    val in=args(0)
    val out=args(1)
    val conf=new SparkConf().
      set("spark.extraListeners","com.DD.sparksql.cdnAccess.MyListenter")

    val sc=new SparkContext(conf)

    val accessFile=sc.textFile(in,4)
    val accessRdd=accessFile.mapPartitions(x=>x.map(_.split("\t"))
      .map(x=>AccessLog(x(0)
        ,x(1)
        ,x(2)
        ,Integer.parseInt(x(3).replace("-","0"))
        ,x(4)
        ,x(5)
        ,x(6)
        ,x(7)
        ,x(8)
        ,x(9)
        ,x(10)
        ,x(11)
        ,x(12)
      )))

    //accessRdd.take(10).foreach(println(_))
    val accessLogAfter=accessRdd.map(LogParse.parse(_)).filter(x =>x!=null)
    accessLogAfter.map(x => x.toString).saveAsTextFile(out)
    sc.stop()
    MysqlConnect.close()
  }
}

