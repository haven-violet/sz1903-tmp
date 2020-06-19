package com.tags

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Author liaojincheng
  * @Date 2020/6/19 13:41
  * @Version 1.0
  * @Description
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf)
      .getOrCreate()
    //读取停用词库
    val arr: Array[String] = spark.sparkContext.textFile("D:\\学习视频\\千峰大数据后续\\day43-画像项目\\资料\\Spark用户画像分析\\stopwords.txt").collect()
    val stopBroad = spark.sparkContext.broadcast(arr)
    val rdd = spark.sparkContext.parallelize(List("配音语种", "TV版", "黑客帝国", "复仇者联盟", "泰坦尼克号", "神话"))
    //获取关键字,进行过滤
    rdd.filter(word => word.length>=3 && word.length<=8 && !stopBroad.value.contains(word))
      .foreach(println)

  }
}
