package com.util

/**
  * @Author liaojincheng
  * @Date 2020/6/16 18:48
  * @Version 1.0
  * @Description
  */
object TypeUtils {
  //将String转换成int
  def str2Int(str: String) = {
    try{
      str.toInt
    }catch {
      case _:Exception => 0
    }
  }

  //将String转换成double
  def str2Double(str: String) = {
    try{
      str.toDouble
    }catch {
      case _:Exception => 0.0
    }
  }
}
