package com.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * @Author liaojincheng
  * @Date 2020/6/20 20:06
  * @Version 1.0
  * @Description
  * todo jedis数据库连接池
  */
object JedisConnectionPool {
  private val config = new GenericObjectPoolConfig()
  config.setMaxTotal(10)
  config.setMaxIdle(5)
  //sss 密码待定
  private val pool = new JedisPool(config, "hadoop201", 6379, 10000, "hadoop")
  def getConnection() ={
    pool.getResource
  }
}
