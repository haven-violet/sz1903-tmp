package com.test

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @Author liaojincheng
  * @Date 2020/6/17 8:27
  * @Version 1.0
  * @Description
  * 3.2.1 地域分布
  */
object LocationRtp {
  def main(args: Array[String]): Unit = {
    //创建执行入口
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .config(conf)
      .appName("LocationRtp")
      .master("local")
      .getOrCreate()
    val df1 = spark.read.parquet("F:\\project\\TestLog")
    df1.createOrReplaceTempView("log")
    /*
    requestmode:Int 数据请求方式(1.请求 2.展示 3.点击)
    processnode:Int 流程节点(1.请求量kpi  2.有效请求  3.广告请求)
    iseffective:Int 有效标识(有效指可以正常计费的) (0: 无效   1: 有效)
    isbilling:Int 是否收费(0: 未收费   1: 已收费)
    isbid:Int   是否rtb
    iswin:Int   是否竞价成功
    adorderid:Int 广告id
     */
    val df2 = spark.sql(
      s"""
         |select
         |  provincename,
         |  cityname,
         |  sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as ysrequest,
         |  sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as yxrequest,
         |  sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) as adrequest,
         |  sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) as cybid,
         |  sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) as cybidsucc,
         |  sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as shows,
         |  sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as clicks,
         |  sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) as dspcost,
         |  sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) as dspapy
         |from log
         |group by provincename, cityname
       """.stripMargin)
    //将结果写入Mysql数据库
    //加载配置文件(conf,json,properties)
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user", load.getString("jdbc.user"))
    prop.setProperty("password", load.getString("jdbc.password"))

    df2.coalesce(1).write.mode(SaveMode.Append).jdbc(
      load.getString("jdbc.url"),
      load.getString("jdbc.TableName"),
      prop)

    /*
    提示: 使用sparkCore来进行统计计算, 提示使用拉链表zip
     */
    val list = List(List(1,2,3,4,5,6,7,8,9), List(1,2,3,4,5,6,7,8,9))
    val rdd: RDD[List[Int]] = spark.sparkContext.parallelize(list)
    //为了后期的reduceByKey,使用1作为主键,来聚合所有List元素
    val wordsRdd: RDD[(Int, List[Int])] = rdd.map((1, _))
    wordsRdd.reduceByKey( (list1, list2) => {
      val tuples: List[(Int, Int)] = list1.zip(list2)
      val list: List[Int] = tuples.map(t => t._1 + t._2)
      list
    }).foreach(println)


  }
}
