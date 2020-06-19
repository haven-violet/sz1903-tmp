package com.tags

import org.apache.spark.sql.Row

/**
  * @Author liaojincheng
  * @Date 2020/6/19 15:56
  * @Version 1.0
  * @Description
  */
object TagsHome extends Tags{
  def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args.asInstanceOf[Row]

    list
  }
}
