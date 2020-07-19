package com.geek.quickStart

import com.geek.DAO.DeptDAO
import com.geek.quickStart.SourceFunctions.{Access, ListOutFunction}
import com.geek.utils.mysql.MysqlConnectPool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import scala.collection.mutable.ListBuffer


object ConnectorsApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //redisSink(env)

    mysqlSink(env)
    env.execute(this.getClass.getSimpleName)
  }

  def redisSink(env:StreamExecutionEnvironment): Unit ={

    val redisStream=env.addSource[Access](new ListOutFunction()).
      keyBy("domain").
      timeWindow(Time.seconds(5)).
      sum("traffic").map(x=>{(x.domain,x.traffic.toInt)})

    val conf = new FlinkJedisPoolConfig.Builder().setHost("yqdata000").setPort(16379).build()

    redisStream.addSink(new RedisSink[(String, Int)](conf, new RedisExampleMapper))
  }

  def mysqlSink(env:StreamExecutionEnvironment): Unit ={

    val redisStream=env.addSource[Access](new ListOutFunction()).
      keyBy("domain").
      timeWindow(Time.seconds(5)).
      sum("traffic").
      map(x=>{(x.domain,x.traffic.toInt)}).
      addSink(new MysqlCustomerSink)

  }

}


class MysqlCustomerSink extends RichSinkFunction[(String, Int)]{

  var deptDAO:DeptDAO = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    MysqlConnectPool.yqdataPoolSetup()
    deptDAO=new DeptDAO
  }

  override def invoke(value: (String, Int)): Unit = {
    val list=new ListBuffer[Seq[Any]]
    list.append(Seq(value._1,value._2))
    deptDAO.bacthInsert(list)
  }

  override def close(): Unit = {
    super.close()
    MysqlConnectPool.yqdataPoolColse()
  }
}


class RedisExampleMapper extends RedisMapper[(String, Int)]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "flink_redis_access")
  }

  override def getKeyFromData(data: (String, Int)): String = data._1

  override def getValueFromData(data: (String, Int)): String = data._2+""
}
