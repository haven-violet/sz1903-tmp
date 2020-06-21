package com.tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * @Author liaojincheng
  * @Date 2020/6/19 10:21
  * @Version 1.0
  * @Description
  */
object TagsKeyValue extends Tags{
  def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val stopBroad = args(1).asInstanceOf[Broadcast[Array[String]]]
    /*
    TODO 关键字(标签格式: Kxxx -> 1)xxx为关键字,关键字个数不能少于3个字符,且
         不能超过8个字符;关键字中如包含"|",则分割为数组,转化成多个关键字标签
         1.先split切分
         2.filter >=3 && <=8   &&   !stopBroad.contains(word)
         3.加上K    map需要有返回值; foreachd不需要有返回值

         0bb49045000057eee4ed3a580019ca06,0,1,0,100002,未知,26C07B8C83DB4B6197CEB80D53B3F5DA,
         1,2,0,0,2016-10-0106:19:17,139.227.161.115,com.apptreehot.horse,马上赚钱,
         AQ+KIQeBhehxf6x988FFnl+CV00p,A10%E5%8F%8C%E6%A0%B8,1,4.1.1,,768,980,
         114.367382561,25.827816529,上海市,上海市,4,未知,3,Wifi,1,0,2,插屏,1,3,6,
         未知,1,0,5585,0,0,0,0,0,4ade8c0b4c,5hqo6b1o3d,7wqc0a7h2e,5wqb5h1b3c,
         8zem7a4t05,,,,,,,0,555,240,290,,,,,,,,,,,AQ+KIQeBhehxf6x988FFnl+CV00p,,
         1,0,0,0,0,7359,,,mm_26632353_8068780_27326559,2016-10-0106:19:17,,
         keywords: String,
     */
    val arr = row.getAs[String]("keywords").split("\\|")
    val filterArr = arr.filter(word => word.length>=3 && word.length<=8 && !stopBroad.value.contains(word)).foreach(word => {
      list:+= ("K" + word, 1)
    })
    list
  }
}
