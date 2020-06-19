package com.tags

import org.apache.spark.sql.Row

/**
  * @Author liaojincheng
  * @Date 2020/6/19 0:17
  * @Version 1.0
  * @Description
  */
object TagsDev extends Tags{
  def makeTags(args: Any*): List[(String, Int)] = {
    /*
    TODO
      4.设备
        a) 操作系统->1
        b) 联网方->1
        c) 运营商->1
     */
    var list = List[(String, Int)]()
    val row = args.asInstanceOf[Row]
    val client = row.getAs[Int]("client")
    client match {
      case 1 => list:+=("D00010001", 1)
      case 2 => list:+=("D00010002", 1)
      case 3 => list:+=("D00010003", 1)
      case _ => list:+=("D00010004", 1)
    }

    val networkname = row.getAs[String]("networkmannername")
    networkname match {
      case "wifi" => list:+=("D00020001", 1)
      case "4G" => list:+=("D00020002", 1)
      case "3G" => list:+=("D00020003", 1)
      case "2G" => list:+=("D00020004", 1)
      case _ => list:+=("D00020005", 1)
    }

    val ispname = row.getAs[String]("ispname")
    ispname match {
      case "移动" => list:+=("D00030001" , 1)
      case "联通" => list:+=("D00030002", 1)
      case "电信" => list:+=("D00030003", 1)
      case _ => list:+=("D00030004", 1)
    }
    list

  }
}
