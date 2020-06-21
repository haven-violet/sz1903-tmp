package com.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * @Author liaojincheng
  * @Date 2020/6/18 21:28
  * @Version 1.0
  * @Description
  */
object TagsApp extends Tags{
  def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    /*
    TODO
      App名称 (标签格式: APPxxx -> 1) xxx为app名称, 使用缓存文件appname_dict
      进行名称转换; App爱奇艺 -> 1
     */
    val row = args(0).asInstanceOf[Row]
    val dictBroad = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]
    val appid = row.getAs[String]("appid")
    var appname = row.getAs[String]("appname")
    //如果appname没有从字典中获取
    if(StringUtils.isBlank(appname)){
      appname = dictBroad.value.getOrElse(appid, "其他")
    }
    list:+=("App"+appname, 1)
    list
  }
}
