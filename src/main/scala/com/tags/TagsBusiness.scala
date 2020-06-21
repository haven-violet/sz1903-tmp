package com.tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, TypeUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * @Author liaojincheng
  * @Date 2020/6/20 10:35
  * @Version 1.0
  * @Description
  */
object TagsBusiness extends Tags{

  def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]
    //获取经纬度
    /*
    TODO 问题1: 就是有些经纬度像这样 119.268188687119.268188687119.268188687，不能够解析，报multiple points
         问题2: 经纬度的范围不是中国的范围
     */

//    val long = row.getAs[String]("longs").toDouble
//    val lat = row.getAs[String]("lat").toDouble
    if(TypeUtils.str2Double(row.getAs[String]("longs")) > 73
     &&TypeUtils.str2Double(row.getAs[String]("longs")) < 153
     &&TypeUtils.str2Double(row.getAs[String]("lat")) > 3
     &&TypeUtils.str2Double(row.getAs[String]("lat")) < 53){
      val long = TypeUtils.str2Double(row.getAs[String]("longs"))
      val lat = TypeUtils.str2Double(row.getAs[String]("lat"))

      val business = getBusiness(long, lat, jedis)
      if(StringUtils.isNotBlank(business)){
        val arr = business.split(",")
        arr.foreach(str => {
          list:+=(str, 1)
        })
      }

    }

    //通过经纬度获取商圈
//    val business = AmapUtil.getBusinessFromAmap(long, lat)
//    business.foreach(str => {
//      list:+=(str, 1)
//    })

/*
在实际上计算支出费用的时候发现每次调用getBussinessFromAmap(long, lat)
来获取商圈的时候,都是会进行收费的,在高德地图上每5000万次要收费6万,
那么老板叫你给公司省钱,你怎么做?
其实每次请求(long, lat),肯定会有重复或者相近区域的,这时候是可以使用
数据库来进行缓存的，首先先将标签进行缓存,下一次在获取商圈的时候先去
缓存中访问一次，如果缓存中没有的话，在调用getBussinessFromAmap()来
进行获取,获取好后，在保存到缓存中,这样可以省下不少钱,
但是选择什么来做redis中的key呢?
1.选择经纬度来做key,但是经纬度太多了,不适合,因为相差不大但是商圈可能
就会不一样。
2.省市区等来做key,但是不够精确
3.GeoHash算法来,会将类似范围的经纬度进行编码归到一起,返回同一个编码字符串
经纬度分区,二分查找类似,左边的为0，右边的为1
经纬度组合,偶数放经度,奇数放纬度,组合后的字符串还会经过base32来
进行编码,类似的经纬度范围返回的是同一个编码字符串

 */
    /*
    TODO
      通过经纬度获取商圈
      如果直接访问高德第三方插件,是收费的,所以不能直接这么做,太烧钱了
      所以我们提出一个方案,将标签进行缓存,如果下一次再次获取商圈的时候
      先去缓存访问一次,没有的话,再去高德获取,如果有直接拿去,不用访问高德
      缓存数据 -> redis
      问题: key如何设置?
      1.经纬度为key -> 因为太多了
      2.省市为key -> 精度不准确
      3.使用GeoHash算法 -> 将经纬度转换成Geo编码来当key
     什么是GeoHash算法?
      先 经纬度分区,类似二分查找,左边为0,右边为1
      后 进行组码,偶数放经度,奇数放纬度, 使用base32编码成5,6位字符
      5个为一个十进制数字
     */
    //先从redis数据库中获取商圈

    list
  }


  /**
    * 获取商圈，先去数据库中查找,数据库中没有再从高德地图中查找
    * @param long
    * @param lat
    * @param jedis
    */
  def getBusiness(long: Double, lat: Double, jedis: Jedis) = {
    //转码
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 6)
    //先去数据库中查找商圈
    var business = redis_queryBusiness(geoHash, jedis)
    //如果数据库中没查到
    if(business == null || business.length == 0){
      //第一次为null; 或者business==""
      //只能从高德中查询了
      business = AmapUtil.getBusinessFromAmap(long, lat)
      //请求完成后,在将此商圈存入数据库一份,为了下次使用
      if(business != null || business.length > 0){
        redis_insertBusiness(geoHash, business, jedis)
      }
    }
    business
  }

  /**
    * 数据库查询
    * @param geoHash
    * @param jedis
    */
  def redis_queryBusiness(geoHash: String, jedis: Jedis) = {
    jedis.get(geoHash)
  }

  /**
    * 数据库插入
    * @param geoHash
    * @param business
    * @param jedis
    * @return
    */
  def redis_insertBusiness(geoHash: String, business: String, jedis: Jedis) = {
    jedis.set(geoHash, business)
  }


}
