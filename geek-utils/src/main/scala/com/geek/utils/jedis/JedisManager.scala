package com.geek.utils.jedis

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

class JedisManager {
  def getJedisPool= {
    val config = new JedisPoolConfig()
    config.setMaxIdle(3)
    config.setMaxTotal(10)
    new JedisPool(config,"yqdata000",16379,2000)
  }

  def getCustomerJedisPool(host:String,port:Int,timeout:Int)={
    val config = new JedisPoolConfig()
    config.setMaxIdle(3)
    config.setMaxTotal(10)
    newJedisPool(config,host,port,timeout)
  }

  def newJedisPool(config:JedisPoolConfig,host:String,port:Int,timeout:Int) ={
    new JedisPool(config,host,port,timeout)
  }
}

object JedisManager{
  private val jedisManagerInstance ={
    new JedisManager()
  }
  lazy val jedisPool= jedisManagerInstance.getJedisPool

  def getRedis()  ={
    val pool=jedisPool
    pool.getResource
  }

  def close(jedis:Jedis): Unit ={
    if(jedis!=null)
      jedis.close()
  }

  def clean(jedis:Jedis): Unit ={
    this.close(jedis)
    if(jedisPool!=null){
      jedisPool.close()
    }
  }
}

object JedisCustomerManager{
  private val jedisManagerInstance ={
    new JedisManager()
  }
  private var jedisPool:JedisPool= _

  def getRedis(host:String,port:Int,timeout:Int)  ={
    if(jedisPool ==null){
      jedisPool=jedisManagerInstance.getCustomerJedisPool(host,port,timeout)
    }
    jedisPool.getResource
  }

  def close(jedis:Jedis): Unit ={
    if(jedis!=null)
      jedis.close()
  }

  def clean(jedis:Jedis): Unit ={
    this.close(jedis)
    if(jedisPool!=null){
      jedisPool.close()
    }
  }
}