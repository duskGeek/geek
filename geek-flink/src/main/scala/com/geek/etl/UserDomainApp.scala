package com.geek.etl

import java.sql.{Connection, PreparedStatement}
import java.util.concurrent.{Callable, ExecutorService, TimeUnit}

import com.alibaba.druid.pool.DruidDataSource
import com.alibaba.fastjson.JSON
import com.geek.DAO.UserDomainDAO
import com.geek.utils.HttpClientUtil
import com.geek.utils.mysql.MysqlConnectPool
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{AsyncDataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import scala.concurrent.{ExecutionContext, Future}

object UserDomainApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val param=ParameterTool.fromArgs(args)

    //userDomainMapping(env,param)
    //ipConvertCity(env,param)
    asyncUserDomainMapping(env,param)
    env.execute(this.getClass.getSimpleName)
  }

  def userDomainMapping(env:StreamExecutionEnvironment,param:ParameterTool): Unit ={

    val port=param.getRequired("port")
    val host=param.getRequired("host")

    env.socketTextStream(host,port.toInt).map(x => {
      x.split(',')
    }).map(x=>{
      try{
        Access(x(0),x(1),x(2),x(3).toInt,0)
      }catch {
        case e => e.printStackTrace();null
      }
    }).filter(_!=null).map(new RichMapFunction[Access,Access] {
      var userDomainDAO:UserDomainDAO = _
      override def open(parameters: Configuration): Unit = {
        MysqlConnectPool.yqdataPoolSetup()
        userDomainDAO=new UserDomainDAO
      }

      override def map(value: Access): Access = {
        val userId=userDomainDAO.findUserIdByDomain(value.domain)
        value.copy(userId=userId.getOrElse(0))
      }
      override def close(): Unit = {
        MysqlConnectPool.yqdataPoolColse()
      }
    }).print()
  }


  def ipConvertCity(env:StreamExecutionEnvironment,param:ParameterTool): Unit ={

    val port=param.getRequired("port")
    val host=param.getRequired("host")

    env.socketTextStream(host,port.toInt).map(x => {
      x.split(',')
    }).map(x=>{
      try{
        Access2(x(0),x(1),x(2).toInt,x(3),x(4),"","","")
      }catch {
        case e => e.printStackTrace();null
      }
    }).filter(_!=null).map(new RichMapFunction[Access2,Access2] {
      override def open(parameters: Configuration): Unit = {

      }

      override def map(value: Access2): Access2 = {
        val loc=value.longitude+","+value.latitude
        val param=Map("key"->"0139ab9797c4367a167432510b8a62b6" ,
          "location" -> loc )
        import scala.collection.JavaConversions._
        val respose=HttpClientUtil.get("https://restapi.amap.com/v3/geocode/regeo",param)
        println("respose:"+respose)

        val reObj=JSON.parseObject(respose)
        val access2=reObj.get("status") match {
          case "1" =>value.copy(province = reObj.getJSONObject("regeocode").getJSONObject("addressComponent").getString("province"),
            city =reObj.getJSONObject("regeocode").getJSONObject("addressComponent").getString("city"),
            district =reObj.getJSONObject("regeocode").getJSONObject("addressComponent").getString("district")
          )
          case  _ =>value
        }
        access2
      }
      override def close(): Unit = {
      }
    }).print()
  }

  def asyncUserDomainMapping(env:StreamExecutionEnvironment,param:ParameterTool): Unit ={

    val port=param.getRequired("port")
    val host=param.getRequired("host")

    val accessDs=env.socketTextStream(host,port.toInt).map(x => {
      x.split(',')
    }).map(x=>{
      try{
        Access(x(0),x(1),x(2),x(3).toInt,0)
      }catch {
        case e => e.printStackTrace();null
      }
    }).filter(_!=null)

    val result=AsyncDataStream.unorderedWait(accessDs,new MyRichAsyncFunction,1000,TimeUnit.MILLISECONDS,10)
    result.print()

  }

  class MyRichAsyncFunction extends RichAsyncFunction[Access,Access] {

    var duridPool :DruidDataSource= _

    implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

    var executorService:ExecutorService = _

    override def open(parameters: Configuration): Unit = {
      duridPool=new DruidDataSource();
      duridPool.setDriverClassName("com.mysql.jdbc.Driver")
      duridPool.setUsername("root")
      duridPool.setPassword("yq1234!@#$")
      duridPool.setUrl("jdbc:mysql://yqdata000:3306/yqdata?useUnicode=true&characterEncoding=utf-8&useSSL=false")
      duridPool.setInitialSize(5)
      duridPool.setMinIdle(5)
      duridPool.setMaxActive(20)

      executorService=java.util.concurrent.Executors.newFixedThreadPool(5)

    }

    override def close(): Unit = {
      duridPool.close()
      executorService.shutdown()
    }


    override def asyncInvoke(input: Access, resultFuture: ResultFuture[Access]): Unit = {

      var future: java.util.concurrent.Future[Access] = executorService.submit(new Callable[Access] {
        override def call(): Access = {
          query(input)
        }
      })
      val request=Future{
        future.get()
      }
      request.onSuccess{
        case result:Access => resultFuture.complete(Iterable(result))
      }
    }

    def query(access:Access): Access ={
      var conn:Connection= null
      val sql="select user_id from user_domain where domain=?"
      var pstm:PreparedStatement = null
      var userId=0
      try{
        conn=this.duridPool.getConnection
        pstm=conn.prepareStatement(sql)
        pstm.setString(1,access.domain)
        val rs=pstm.executeQuery()
        while (rs.next()){
          userId=rs.getInt(1)
        }
      }catch {
        case e =>e.printStackTrace()
      }
      access.copy(userId=userId)
    }

  }


}

case class Access(time:String,domain:String,city:String,traffic:Int,userId:Int)

case class Access2(time:String,domain:String,traffic:Int,longitude:String,latitude:String,
                   province:String,city:String, district:String)
