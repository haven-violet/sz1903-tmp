package com.util

/**
  * @Author liaojincheng
  * @Date 2020/6/20 11:15
  * @Version 1.0
  * @Description 从高德获取商圈信息
  */
object AmapUtil {
  def getBusinessFromAmap(long: Double, lat:Double) = {
    //https://restapi.amap.com/v3/geocode/regeo
    //?location=116.310003,39.991957&key=618fdc90d87d773647cef2165aab3763&radius=3000&extensions=base
    //拼接url
    val location = long + "," + lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=618fdc90d87d773647cef2165aab3763&radius=3000&extensions=base"
    //发送HTTP请求协议
    val json = HttpUtilTest.get(url)
   /*{
      "status": "1",
      "regeocode": {
        "addressComponent": {
        "city": [],
        "province": "北京市",
        "adcode": "110108",
        "district": "海淀区",
        "towncode": "110108015000",
        "streetNumber": {
        "number": "5号",
        "location": "116.310454,39.9927339",
        "direction": "东北",
        "distance": "94.5489",
        "street": "颐和园路"
      },
        "country": "中国",
        "township": "燕园街道",
        "businessAreas": [
      {
        "location": "116.303364,39.97641",
        "name": "万泉河",
        "id": "110108"
      },
      {
        "location": "116.314222,39.98249",
        "name": "中关村",
        "id": "110108"
      },
      {
        "location": "116.294214,39.99685",
        "name": "西苑",
        "id": "110108"
      }
        ],
        "building": {
        "name": "北京大学",
        "type": "科教文化服务;学校;高等院校"
      },
        "neighborhood": {
        "name": "北京大学",
        "type": "科教文化服务;学校;高等院校"
      },
        "citycode": "010"
      },
        "formatted_address": "北京市海淀区燕园街道北京大学"
      },
      "info": "OK",
      "infocode": "10000"
    }*/

    println(json)
  }
}
