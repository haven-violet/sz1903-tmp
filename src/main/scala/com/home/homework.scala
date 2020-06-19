package com.home

import java.util
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.tags.TagsHome
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.ListBuffer

/**
  * @Author liaojincheng
  * @Date 2020/6/19 15:28
  * @Version 1.0
  * @Description
  * json数据归纳格式（考虑获取到数据的成功因素 status=1成功 status=0 失败）：
  * 1、按照pois，分类businessarea，并统计每个businessarea的总数。
  * 2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
  * 标签结果格式(标签名称,count值)
  */
object homework {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .config(conf)
      .appName("homework")
      .master("local")
      .getOrCreate()

    //val json: JSONObject = JSON.parseObject("F:\\教学视频\\千峰大数据\\练习题\\Test_002\\json.log")
    //val array: JSONArray = json.getJSONObject("regeocode").getJSONArray("pois")
    /*
    sss
      sparkCore可以读取出来的是RDD[String]  所以选择使用sparkCore来读取json格式文件
      sparkSql可以读取出来的是DataFrame
     */
    val rdd: RDD[String] = spark.sparkContext.textFile("F:\\教学视频\\千峰大数据\\练习题\\Test_002\\json.log")
    /*
    TODO
      1.需要考虑到获取的数据成功因素 status = 1成功 status = 0 失败
        先要过滤 filter => JSON.parserObject => status来进行 => if判断
     */
    val filterRdd: RDD[String] = rdd.filter(str => {
      val json = JSON.parseObject(str)
      //TODO: 注意使用==的时候，两边的数据类型必须要一致
      json.get("status").toString == "1" //为true留下来; 为false过滤掉
    })
    /*
    TODO
      2.按照pois, 分类businessarea,并统计每个businessarea的总数
      {
        "status":"1",
        "regeocode":{
            x,
            ...,
            "pois":[{},{}]
        },
        "info":"ok",
        "infocode":"1000"
      }
     */
    filterRdd.map(str => {
      val json = JSON.parseObject(str)
      val regeocode: JSONObject = json.getJSONObject("regeocode")
      val array: JSONArray = regeocode.getJSONArray("pois")
      //选择使用iterator, 而不是array,因为可能array为空没有值,调用array(0)会报错
      val iter = array.iterator()
      val value = JSON.parseObject(iter.next().toString).get("businessarea")
      (value, array.size())
    }).filter(!_._1.toString.equals("[]")).foreach(println)

    /*
    TODO list[String, Int]
      3.按照pois, 分类type,为每一个type类型打上标签,统计各个标签的数量
      标签结果格式(标签名称, count值)
      filterRdd => str(每条记录) => JSON.parseObject(str).getJSONObject("regeocode").getJSONArray("pois")
      => .iterator => { hasNext =>  type.split(";") => list:+=(type,1)
      => 每一个filterRdd list
      => list(list) => flatMap => reduceByKey(_+_) => sortBy(_._2, false) => foreach
      个人小结: 本质上还是一个wordCount思路
     */

    var list = List[(String, Int)]()
    filterRdd.map(str => {
      //json解析每条记录str为JSON对象
      val json: JSONObject = JSON.parseObject(str)
      val array = json.getJSONObject("regeocode").getJSONArray("pois")
      val iter = array.iterator()
      while(iter.hasNext()){
        val nObject = JSON.parseObject(iter.next().toString)
        val types = nObject.getString("type").split(";")
        for(t <- types){
          list:+=(t, 1)
        }
      }
      //list 单个的list是不能够使用reduceByKey的,因为reduceByKey是针对KV形式的元素
      (list, null)
      /*
      RDD[(List[(String, Int)], Null)]
      RDD[(String, Int)]
      没有使用(list, null)， 而是使用list
      RDD(Nothing)
      flatMap,reduceByKey,sortBy都是转化算子
      countByKey,reduce都是执行算子
       */
    }).flatMap(_._1).reduceByKey(_+_).sortBy(_._2).foreach(println)

  }
}
