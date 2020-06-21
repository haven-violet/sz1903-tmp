package com.util

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
//    val conf = new SparkConf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    val spark = SparkSession.builder()
//      .appName("log2Parquet")
//      .master("local")
//      .config(conf)
//      .getOrCreate()
//    //读取停用词库
//    /*
//    电视剧
//    最新更新
//    TV版
//    配音语种
//     */
//    val arr: Array[String] = spark.sparkContext.textFile("D:\\学习视频\\千峰大数据后续\\day43-画像项目\\资料\\Spark用户画像分析\\stopwords.txt").collect()
//    val stopBroad = spark.sparkContext.broadcast(arr)
//    val rdd = spark.sparkContext.parallelize(List("配音语种", "TV版", "黑客帝国", "复仇者联盟", "泰坦尼克号", "神话"))
//    //获取关键字,进行过滤
//    rdd.filter(word => word.length>=3 && word.length<=8 && !stopBroad.value.contains(word))
//      .foreach(println)
//    AmapUtil.getBusinessFromAmap(116.310003,39.991957)
    val conf = new SparkConf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf)
      .getOrCreate()
    //    spark.sparkContext.parallelize(Array(("user1", List(("爱奇艺", 1),("优酷", 1),("腾讯", 1), ("爱奇艺", 1))), ("user1", List(("爱奇艺", 1),("优酷", 1), ("腾讯",1)))))
    //      .reduceByKey((list1, list2) => {
    //        //        val list: List[((String, Int), (String, Int))] = list1.zip(list2)
    //        //        list.map(t => (t._1._1, t._1._2 + t._2._2))
    //        /*
    //        TODO (user1, List(("a", 1),("b", 1))) (user1, List(("a", 1),("b", 1))) 通过groupBy 根据user1进行分组 (user1, List(("a",1),("b",1),("a",1),("b",1)))
    //             而后通过mapValues() 自动将 (user1, ) 和 自动将 (user1, List(("a")))下面的List中的key归并到一列中， 只需要看成List(("a", 1), ...)
    //         */
    //        val stringToTuples: Map[String, List[(String, Int)]] = (list1:::list2).groupBy(_._1)
    //        val stringToInt: Map[String, Int] = stringToTuples.mapValues(_.foldLeft(0)(_+_._2))
    //        stringToInt.toList
    ////        TODO reduceByKey只对key-value有效，对List((),(),...)没有效果
    ////          所以需要换成groupBy和mapValues
    //      }).foreach(println)

    //count()计算元素的个数
//    val rdd: RDD[Int] = spark.sparkContext.parallelize(List(1,3,2,4,5))
//    println(rdd.count())
    //countByKey()
//    val rdd: RDD[(Int, Int)] = spark.sparkContext.parallelize(List((1, 1), (1, 2), (1, 3), (2, 1), (2,2), (3, 3)))
//    //针对k-v类型,按照k分组,而后统计分组后的个数
//    println(rdd.countByKey())
//    val b = spark.sparkContext.parallelize(List("dog", "sd", "zfdfdfdf","cat", "ape", "salmon", "gnu"), 2)
    /*
    take是按照集合顺序来显示前n条数据
    top是将排序后的集合中取出最大前n条数据
    takeOrdered是和top相反的,取出排序后的集合中最小前n条数据
     */
//    val b = spark.sparkContext.parallelize(List(1, 3, 2, 4, 7), 2)
//    b.top(3).foreach(println)
//    println()
//    b.take(3).foreach(println)
//    println()
//    b.takeOrdered(3).foreach(println)

  }
}
