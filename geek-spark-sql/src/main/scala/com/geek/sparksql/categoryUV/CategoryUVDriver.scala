package com.geek.sparksql.categoryUV

import java.sql.{Connection, PreparedStatement}

import com.geek.utils.ContextUtils
import com.geek.utils.mysql.MysqlConnect
import org.apache.spark.sql.SaveMode

import scala.collection.mutable.ListBuffer

object CategoryUVDriver {

  var statement: PreparedStatement  = _

  def main(args: Array[String]): Unit = {
    val spark=ContextUtils.getSparkSession(this.getClass.getSimpleName,"yarn-client")

    val flwDf=spark.read.format("jdbc").
      option("user","mocard_rw").
      option("password","wsxQASW!").
      option("url","jdbc:mysql://172.23.0.39:4000/etl_mocard?useUnicode=true&characterEncoding=utf-8&useSSL=false").
      option("driver","com.mysql.jdbc.Driver").
      option("dbtable","mocard_flw.rt_member_miniapp_flw_detail").load()

    val categoryDf=spark.read.format("jdbc").
      option("user","mocard_rw").
      option("password","wsxQASW!").
      option("url","jdbc:mysql://172.23.0.39:4000/etl_mocard?useUnicode=true&characterEncoding=utf-8&useSSL=false").
      option("driver","com.mysql.jdbc.Driver").
      option("dbtable","mocard_member.member_goods_category").load()

    val orderInfoDf=spark.read.format("jdbc").
      option("user","mocard_rw").
      option("password","wsxQASW!").
      option("url","jdbc:mysql://172.23.0.39:4000/etl_mocard?useUnicode=true&characterEncoding=utf-8&useSSL=false").
      option("driver","com.mysql.jdbc.Driver").
      option("dbtable","mocard_member.mo_order_order_info").load()

    val orderDetailDf=spark.read.format("jdbc").
      option("user","mocard_rw").
      option("password","wsxQASW!").
      option("url","jdbc:mysql://172.23.0.39:4000/etl_mocard?useUnicode=true&characterEncoding=utf-8&useSSL=false").
      option("driver","com.mysql.jdbc.Driver").
      option("dbtable","mocard_member.mo_order_order_detail_info").load()

    val skuDf=spark.read.format("jdbc").
      option("user","mocard_rw").
      option("password","wsxQASW!").
      option("url","jdbc:mysql://172.23.0.39:4000/etl_mocard?useUnicode=true&characterEncoding=utf-8&useSSL=false").
      option("driver","com.mysql.jdbc.Driver").
      option("dbtable","mocard_member.member_goods_sku").load()

    val spuDf=spark.read.format("jdbc").
      option("user","mocard_rw").
      option("password","wsxQASW!").
      option("url","jdbc:mysql://172.23.0.39:4000/etl_mocard?useUnicode=true&characterEncoding=utf-8&useSSL=false").
      option("driver","com.mysql.jdbc.Driver").
      option("dbtable","mocard_member.member_goods_spu").load()


    categoryDf.createTempView("member_goods_category")
    flwDf.createTempView("rt_member_miniapp_flw_detail")
    orderInfoDf.createTempView("mo_order_order_info")
    orderDetailDf.createTempView("mo_order_order_detail_info")
    skuDf.createTempView("member_goods_sku")
    spuDf.createTempView("member_goods_spu")

    //spark.sql("select * from rt_member_miniapp_flw_detail").show(10)

    val df=spark.sql(CategorySql.selectUV)

    df.rdd.foreachPartition(x=>{
      val categories = new ListBuffer[Category]
      x.foreach(y=>{
        val category=Category(y.getAs[String]("category_name"),
          y.getAs[Long]("uv"),
          y.getAs[Long]("pay_num"))
        categories.append(category)
      })
      insertCategory(categories)
    })

//    df.write.format("jdbc").
//      option("url", "jdbc:mysql://172.23.0.39:4001/etl_mocard?useUnicode=true&characterEncoding=utf-8&useSSL=false").
//      option("dbtable", "etl_mocard.ads_memeber_category_statistics").
//      option("user", "mocard_rw").
//      option("password", "wsxQASW!").
//      option("truncate", "true").
//      option("driver","com.mysql.jdbc.Driver").mode(SaveMode.Overwrite).save()


    MysqlConnect.close()
    spark.stop()

  }

  def insertCategory(list:ListBuffer[Category]): Unit ={
    var conn:Connection=null
    var pstm:PreparedStatement=null
    try{
      conn=MysqlConnect.getConnection()
      conn.setAutoCommit(false)
      val sql="REPLACE INTO etl_mocard.ads_memeber_category_statistics(category_name,uv,pay_num) values(?,?,?);"
      val pstm=conn.prepareStatement(sql)
      list.foreach(category=>{
        pstm.setString(1,category.categoryName)
        pstm.setLong(2,category.uv)
        pstm.setLong(3,category.pay_num)

        pstm.addBatch()
      })
      pstm.executeBatch()
      conn.commit()
    }catch{
      case e:Exception =>e.printStackTrace()
    }finally {
      MysqlConnect.release(conn,pstm)
    }

  }

}

case class Category(categoryName:String,uv:Long,pay_num:Long)
