package com.tags

import com.util.TagUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
  * @Author liaojincheng
  * @Date 2020/6/18 16:12
  * @Version 1.0
  * @Description
  *
  * 上下文标签, 用于合并总标签
  * 1.Tags trait特质,类似于接口
  * 2.TagUtils 两个方法 判断用户id是否唯一, 获取用户唯一不为空的id 模式匹配
  * 3.TagsContext 先获取parquet格式文件,读取出来
  *  先判断用户id是否存在,filter出来
  *  后获取用户不为空的id
  *  对每条记录打上广告类型标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("TagsContext")
      .master("local")
      .config(conf)
      .getOrCreate()
    /*
    F:\project\TestLog
    D:\学习视频\千峰大数据后续\day43-画像项目\资料\Spark用户画像分析\app_dict.txt
     */
    val Array(inputPath, dict, stopWords) = args
    //解析字典文件app_dict
    val dictMap: collection.Map[String, String] = spark.sparkContext.textFile(dict).map(_.split("\t", -1)).filter(_.length >= 5)
      .map(t => {
        (t(4), t(1))
      }).collectAsMap()
    //广播字典文件
    val dictBroad: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(dictMap)
    //读取停用词库
    val arr = spark.sparkContext.textFile(stopWords).collect()
    val stopBroad: Broadcast[Array[String]] = spark.sparkContext.broadcast(arr)

    //TODO 读取parquet格式文件
    val df = spark.read.parquet(inputPath)
    //1.先过滤出没有用户唯一id的数据
    df.filter(TagUtils.OneUserId).rdd.map(row => {
      //2.获取不为空的唯一UserId
      val userId = TagUtils.getAnyOneUserId(row)
      //TODO 广告类型标签
      //     渠道标签
      val adList: List[(String, Int)] = TagsAD.makeTags(row)
      /*
      TODO app名称标签
        1.先广播Dict字典
        2.(row, dict)一起传入
       */
      //sss
      val appList = TagsApp.makeTags(row, dictBroad)
      //TODO 设备标签
      TagsDev.makeTags(row)
      //TODO 关键字标签
      TagsKeyValue.makeTags(row)

    })

  }
}
