package com.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * @Author liaojincheng
  * @Date 2020/6/18 16:42
  * @Version 1.0
  * @Description
  */
object TagUtils {
  def getAnyOneUserId(row: Row) = {
    //模式匹配
    row match {
      case v: Row if(StringUtils.isNotBlank(v.getAs[String]("imei"))) => "TM" + v.getAs[String]("imei")
      case v: Row if(StringUtils.isNotBlank(v.getAs[String]("mac"))) => "MC" + v.getAs[String]("mac")
      case v: Row if(StringUtils.isNotBlank(v.getAs[String]("idfa"))) => "ID" + v.getAs[String]("idfa")
      case v: Row if(StringUtils.isNotBlank(v.getAs[String]("openudid"))) => "OD" + v.getAs[String]("openudid")
      case v: Row if(StringUtils.isNotBlank(v.getAs[String]("androidid"))) => "AD" + v.getAs[String]("androidid")
    }
  }


  val OneUserId =
    s"""
       |imei != '' or mac != '' or idfa != '' or openudid != '' or androidid != ''
     """.stripMargin


}
