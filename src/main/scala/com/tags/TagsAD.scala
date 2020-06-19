package com.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * @Author liaojincheng
  * @Date 2020/6/18 17:16
  * @Version 1.0
  * @Description
  * 3.4.1 打广告标签
  */
object TagsAD extends Tags{
  def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args.asInstanceOf[Row]
    val adType = row.getAs[Int]("adspacetype")
    val adName = row.getAs[String]("adspacetypename")
    /*
      TODO
        3.4.1广告标签
        广告位类型(标签格式: LC03 -> 1 或者 LC16 -> 1)xx为数字,小于10补0    adspacetype
        把广告位类型名称, LN插屏 -> 1   adspacetypename
       */
    adType match {
       case v if(v > 9) => list:+=("LC"+v,1)
       case v if(v >= 0 && v < 9) => list:+=("LC0"+v, 1)
    }
    //保证名字不为空
    if(StringUtils.isNotBlank(adName)){
      list:+=("LN"+adName, 1)
    }
    /*
    TODO
      3.4.3
      渠道(标签格式: Appxxx -> 1)xxx为app名称, 使用缓存文件appname_dict
      进行名称转换; App爱奇艺 -> 1
     */
    val adplatId: Int = row.getAs[Int]("adplatformproviderid")
    list:+=("CN"+adplatId, 1)
    list
  }
}
