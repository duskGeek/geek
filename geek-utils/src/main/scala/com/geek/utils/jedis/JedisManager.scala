package com.geek.utils.jedis

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

class JedisManager {
  def getJedisPool= {
    val config = new JedisPoolConfig()
    config.setMaxIdle(3)
    config.setMaxTotal(10)
    new JedisPool(config,"yqdata000",16379,2000)
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

  def clean(jedisPool:JedisPool,jedis:Jedis): Unit ={
    this.close(jedis)
    if(jedisPool!=null){
      jedisPool.close()
    }
  }
}