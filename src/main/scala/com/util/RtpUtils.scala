package com.util

/**
  * @Author liaojincheng
  * @Date 2020/6/17 21:11
  * @Version 1.0
  * @Description
  */
object RtpUtils {


  /*
  前3个指标
    原始请求数
    有效请求数
    广告请求数
   */
  def requestProcessor(requestmode: Int, processnode: Int): List[Double] = {
    if(requestmode == 1 && processnode == 1){
      List[Double](1,0,0)
    }else if(requestmode == 1 && processnode == 2){
      List[Double](1, 1, 0)
    }else if(requestmode == 1 && processnode == 3){
      List[Double](1, 1, 1)
    }else{
      List[Double](0,0,0)
    }
  }

  /*
  4个指标
  参与竞价数
  竞价成功数
  DSP广告消费
  DSP广告成本
   */
  def isBidAndWin(
                   iseffective: Int,
                   isbilling: Int,
                   isbid: Int,
                   iswin: Int,
                   adorderid: Int,
                   winprice: Double,
                   adpayment: Double): List[Double] = {
    if(iseffective == 1 && isbilling == 1 && isbid == 1){
      if(iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0){
        List[Double](1, 1, winprice/1000, adpayment/1000)
      }else{
        List[Double](1, 0, 0, 0)
      }
    }else{
      List[Double](0, 0, 0, 0)
    }
  }

  /*
  2个指标
  展示数
  点击数
   */
  def showAndClick(requestmode: Int, iseffective: Int): List[Double] = {
    if(requestmode == 2 && iseffective == 1){
      List[Double](1, 0)
    }else if(requestmode == 3 && iseffective == 1){
      List[Double](0, 1)
    }else{
      List[Double](0, 0)
    }
  }

}
