package com.util

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer

/**
  * @Author liaojincheng
  * @Date 2020/6/20 11:15
  * @Version 1.0
  * @Description 从高德获取商圈信息
  */
object AmapUtil {
  /*
  TODO
    该方法是传入经度,维度;
    调用HttpClients发送(经度,维度)请求参数,返回一个json字符串
    使用fastjson解析json字符串,层层解析出business商圈name的集合List
    使用mkString返回一个字符串，这就是需要的商圈了
   */
  def getBusinessFromAmap(long: Double, lat:Double):String = {
    //https://restapi.amap.com/v3/geocode/regeo
    //?location=116.310003,39.991957&key=618fdc90d87d773647cef2165aab3763&radius=3000&extensions=base
    //拼接url
    val location = long + "," + lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=618fdc90d87d773647cef2165aab3763&radius=3000&extensions=base"
    //发送HTTP请求协议
    val json = HttpUtilTest.get(url)
   /*
{
    "status":"1",
    "regeocode":{
        "addressComponent":{
            "city":[

            ],
            "province":"北京市",
            "adcode":"110108",
            "district":"海淀区",
            "towncode":"110108015000",
            "streetNumber":{
                "number":"5号",
                "location":"116.310454,39.9927339",
                "direction":"东北",
                "distance":"94.5489",
                "street":"颐和园路"
            },
            "country":"中国",
            "township":"燕园街道",
            "businessAreas":[
                {
                    "location":"116.303364,39.97641",
                    "name":"万泉河",
                    "id":"110108"
                },
                {
                    "location":"116.314222,39.98249",
                    "name":"中关村",
                    "id":"110108"
                },
                {
                    "location":"116.294214,39.99685",
                    "name":"西苑",
                    "id":"110108"
                }
            ],
            "building":{
                "name":"北京大学",
                "type":"科教文化服务;学校;高等院校"
            },
            "neighborhood":{
                "name":"北京大学",
                "type":"科教文化服务;学校;高等院校"
            },
            "citycode":"010"
        },
        "formatted_address":"北京市海淀区燕园街道北京大学"
    },
    "info":"OK",
    "infocode":"10000"
}
   */
    /*
    TODO 解析json对象
      1. parseJSONObject => 判断status => getJSONObject("regeocode") => getJSONObject("addressComponent")
         => getJSONArray("businessAreas") => 遍历 => 先判断isInstanceOf[JSONObject], 在强转asInstanceOf[JSONObject]
         => getJSONObject("name")
         最后返回只能是String字符串
         因为最后的name商圈可能会有多个,所以需要使用List集合来装载
         集合转化成String使用mkString
     */
    //使用阿里巴巴的fastJson包解析
    /*
    最终返回值只能是String,因为后面要给String打标签，其实就是加上前缀等
     */
    //1.解析json字符串为JSON对象
    val jsonObject = JSON.parseObject(json)
    //2.判断status
    val status = jsonObject.getIntValue("status")
    if(status == 0){
      return ""
    }
    //3.获取regeocode
    val regeocode = jsonObject.getJSONObject("regeocode")
    if(regeocode == null){
      return ""
    }
    //4.获取addressComponent
    val addressComponent = regeocode.getJSONObject("addressComponent")
    if(addressComponent == null){
      return ""
    }
    //5.获取businessAreas
    val businessAreas = addressComponent.getJSONArray("businessAreas")
    if(businessAreas == null){
      return ""
    }
    var list = ListBuffer[String]()
    //6.遍历businessAreas
    for(arr <- businessAreas.toArray()){
      if(arr.isInstanceOf[JSONObject]){
        val nObject = arr.asInstanceOf[JSONObject]
        val name = nObject.getString("name")
        list:+=name
      }
    }
    list.mkString(",")
  }
}
