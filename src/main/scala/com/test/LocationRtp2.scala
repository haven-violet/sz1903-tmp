package com.test

import com.util.RtpUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author liaojincheng
  * @Date 2020/6/17 20:54
  * @Version 1.0
  * @Description
  * 3.2.1 地域分布 使用sparkCore实现
  */
object LocationRtp2 {
  /*
  TODO sparkCore来实现 地域分布统计
    提示: 将df => rdd => Row => 写一个工具类来实现 => List(0,1 ...) => 使用zip拉链表来实现操作
    进行累加 实现和sparkSql中的sum(case when then else end)等效效果
   */
  def main(args: Array[String]): Unit = {
    //创建执行入口
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("locationRtp2")
      .master("local")
      .config(conf)
      .getOrCreate()
    //读取前面已经清洗好的parquet格式文件
    val df1 = spark.read.parquet("F:\\project\\TestLog")

    /**
      * 对每条记录中查看是否符合某些条件， 如果符合 => 1;  如果不符合 => 0;记录一波
      * List(指标1,指标2...)
      */
    df1.rdd.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      val list1: List[Double] = RtpUtils.requestProcessor(requestmode, processnode)
      val list2: List[Double] = RtpUtils.isBidAndWin(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      val list3: List[Double] = RtpUtils.showAndClick(requestmode, iseffective)
      ((row.getAs[String]("provincename"), row.getAs[String]("cityname")), list1 ++ list2 ++ list3)
    })
      //聚合
      .reduceByKey((list1, list2) => {
        list1.zip(list2).map(t => t._1 + t._2)
      }).foreach(println)

    spark.stop()

  }
}
