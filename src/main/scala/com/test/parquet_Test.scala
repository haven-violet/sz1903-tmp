package com.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author liaojincheng
  * @Date 2020/6/16 21:11
  * @Version 1.0
  * @Description
  * 统计各省市数据量分布情况
  * 要求一: 将统计的结果输出成json,并输出到磁盘目录
  * 要求二: 将结果写到mysql数据库
  * 要求三: 用spark算子的方式实现上述的统计,存储到磁盘
  */
object parquet_Test {
  def main(args: Array[String]): Unit = {
    //创建执行入口
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("parquet_test")
      .master("local")
      .config(conf)
      .getOrCreate()

    //TODO 方式一: 使用sparkSql来完成操作
    val df = spark.read.parquet("F:\\project\\TestLog")
    df.createOrReplaceTempView("log")
    val df1 = df.sqlContext.sql(
      s"""
         |select
         |  provincename,
         |  cityname,
         |  count(1) as counts
         |from log
         |group by provincename, cityname
       """.stripMargin)
    //TODO (1) 使用partitionBy
//    df1.write.partitionBy("provincename", "cityname").json("output")

    /**
      *  TODO (2) 使用coalesce
      *   1. RDD算子中
      *     coalesce 缩小分区
      *     repartition 扩大分区, 底层调用coalesce + shuffle = true
      *   2. DataFrame中
      *     coalesce 合并/缩小分区, 底层调用repartition + shuffle = false
      *     repartition
      */
    df1.coalesce(1).write.json("output")

    //TODO 方式二: 使用sparkCore来完成,转换成RDD (Row), 但是df不能直接转换成ds
    df1.rdd.map(row => {
      ((row.getAs[String]("provincename"), row.getAs[String]("cityname")), 1)
    }).reduceByKey(_+_).foreach(println)

    //关闭
    spark.stop()
  }
}
