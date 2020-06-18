package com.tags

import com.util.TagUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author liaojincheng
  * @Date 2020/6/18 16:12
  * @Version 1.0
  * @Description
  *
  *             上下文标签, 用于合并总标签
  *             1.Tags trait特质,类似于接口
  *             2.TagUtils 两个方法 判断用户id是否唯一, 获取用户唯一不为空的id 模式匹配
  *             3.TagsContext 先获取parquet格式文件,读取出来
  *              先判断用户id是否存在,filter出来
  *              后获取用户不为空的id
  *              对每条记录打上广告类型标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("TagsContext")
      .master("local")
      .config(conf)
      .getOrCreate()
    val Array(inputPath) = args

    //TODO 读取parquet格式文件
    val df = spark.read.parquet(inputPath)
    //1.先过滤出没有用户唯一id的数据
    df.filter(TagUtils.OneUserId).rdd.map(row => {
      //2.获取不为空的唯一UserId
      val userId = TagUtils.getAnyOneUserId(row)
      /*
      TODO
        3.4.1广告标签
        广告位类型(标签格式: LC03 -> 1 或者 LC16 -> 1)xx为数字,小于10补0
        把广告位类型名称, LN插屏 -> 1
       */

    })
  }
}
