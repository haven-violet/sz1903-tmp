package com.test

import com.util.RtpUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author liaojincheng
  * @Date 2020/6/17 23:27
  * @Version 1.0
  * @Description
  * 3.2.3媒体分析
  * 媒体维度统计
  */
object AppRpt {
  def main(args: Array[String]): Unit = {
    /*
    1.先解析app_dict.txt字典文件
    2.广播字典文件
    3.读取parquet格式文件
    4.读取appid, appname; 如果没有从字典文件中读取
    果然是修改文件就可以了
     */
    val Array(inputPath, app_dic) = args
    //创建执行入口
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("appRpt")
      .master("local")
      .config(conf)
      .getOrCreate()
    /*
    inputPath F:\project\TestLog
    app_dic D:\学习视频\千峰大数据后续\day43-画像项目\资料\Spark用户画像分析\app_dict.txt
    56	超级捕鱼	A0302	A03	com.hg.dynamitefishinga_noads	一款
     */
    val appDict = spark.sparkContext.textFile(app_dic).map(_.split("\t", -1)).filter(_.length >= 5)
      .map(arr => {
        (arr(4), arr(1))
      }).collectAsMap()
    //进行广播
    val appDictBroad = spark.sparkContext.broadcast(appDict)
    //读取parquet格式文件
    val df = spark.read.parquet(inputPath)
    df.rdd.map(row => {
      val appid = row.getAs[String]("appid")
      val appname = row.getAs[String]("appname")
      //先判断appname是否存在,因为appname可能会不存在,如果不存在,需要从字典文件中进行获取
      if(StringUtils.isBlank(appname)){
        appDictBroad.value.getOrElse(appid, "其他")
      }

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
      (appname, list1 ++ list2 ++ list3)
    })
    //聚合
      .reduceByKey((list1, list2) => {
      val tuples = list1.zip(list2)
      tuples.map(t => {
        t._1 + t._2
      })
    }).foreach(println)

    //关闭
    spark.stop()
  }
}
