package com.tags

import com.typesafe.config.ConfigFactory
import com.util.{JedisConnectionPool, TagUtils}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
  * @Author liaojincheng
  * @Date 2020/6/18 16:12
  * @Version 1.0
  * @Description
  *
  * 上下文标签, 用于合并总标签
  * 1.Tags trait特质,类似于接口
  * 2.TagUtils 两个方法 判断用户id是否唯一, 获取用户唯一不为空的id 模式匹配
  * 3.TagsContext 先获取parquet格式文件,读取出来
  * 先判断用户id是否存在,filter出来
  * 后获取用户不为空的id
  * 对每条记录打上广告类型标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("TagsContext")
      .master("local")
      .config(conf)
      .getOrCreate()
    /*
    F:\project\TestLog
    D:\学习视频\千峰大数据后续\day43-画像项目\资料\Spark用户画像分析\app_dict.txt
    D:\学习视频\千峰大数据后续\day43-画像项目\资料\Spark用户画像分析\stopwords.txt
    F:\project\TestLog D:\学习视频\千峰大数据后续\day43-画像项目\资料\Spark用户画像分析\app_dict.txt D:\学习视频\千峰大数据后续\day43-画像项目\资料\Spark用户画像分析\stopwords.txt
    2020618
     */
    /*
    TODO
     整合hbase
     */
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TabName")
    //加载Hbase配置
    val configuration = spark.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.zk"))
    //如果配置文件内的zk是分开写的,就是端口和host分开,那么要使用下面这样的连接方式
//    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
//    configuration.set("hbase.zookeeper.property.clientPort",load.getString("hbase.port"))
    //创建任务,并获取Connection连接
    val hbconn = ConnectionFactory.createConnection(configuration)
    val admin = hbconn.getAdmin
    //如果当前表不存在,那么我们需要创建一个新表,存储则反之
    if(!admin.tableExists(TableName.valueOf(hbaseTableName))){
      println("创建表~~~")
      //创建表对象
      val tableNameDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      //创建列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      //将列簇加载到表中
      tableNameDescriptor.addFamily(columnDescriptor)
      //创建表
      admin.createTable(tableNameDescriptor)
      //关闭
      admin.close()
      hbconn.close()
    }
    //创建JobConf
    val jobConf = new JobConf(configuration)
    //指定key的输出类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定输出到哪张表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

    val Array(inputPath, dict, stopWords, day) = args
    //解析字典文件app_dict
    val dictMap: collection.Map[String, String] = spark.sparkContext.textFile(dict).map(_.split("\t", -1)).filter(_.length >= 5)
      .map(t => {
        (t(4), t(1))
      }).collectAsMap()
    //广播字典文件
    val dictBroad: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(dictMap)
    //读取停用词库
    val arr = spark.sparkContext.textFile(stopWords).collect()
    val stopBroad: Broadcast[Array[String]] = spark.sparkContext.broadcast(arr)

    //TODO 读取parquet格式文件
    val df = spark.read.parquet(inputPath)
    //1.先过滤出没有用户唯一id的数据
    df.filter(TagUtils.OneUserId)
//      .rdd.foreach(row => println((row.getAs[String]("longs").toDouble, row.getAs[String]("lat").toDouble)))
      .rdd.mapPartitions(iterator => {
      val jedis = JedisConnectionPool.getConnection()
      val ite: Iterator[(String, List[(String, Int)])] = iterator.map(row => {
        //2.获取不为空的唯一UserId
        val userId = TagUtils.getAnyOneUserId(row)
        //TODO 广告类型标签
        //     渠道标签
        //     地域标签
        val adList: List[(String, Int)] = TagsAD.makeTags(row)
        /*
        TODO app名称标签
          1.先广播Dict字典
          2.(row, dict)一起传入
         */
        //sss
        val appList = TagsApp.makeTags(row, dictBroad)
        //TODO 设备标签
        val devList = TagsDev.makeTags(row)
        //TODO 关键字标签
        val kvList = TagsKeyValue.makeTags(row, stopBroad)
        //TODO 商圈标签
        val busList = TagsBusiness.makeTags(row, jedis)
        (userId, adList ++ appList ++ devList ++ kvList ++ busList )
//        (userId, busList )
      })
      jedis.close()
      ite
    }).reduceByKey((list1, list2) => {
      val list: List[((String, Int), (String, Int))] = list1.zip(list2)
      list.map(t=>{
        (t._1._1, t._1._2 + t._2._2)
      })
    }).map{
      case (userId, userTags) => {
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(
          Bytes.toBytes("tags"),
          Bytes.toBytes(day),
          Bytes.toBytes(userTags.mkString(","))
        )
        (new ImmutableBytesWritable(), put)
      }
    } //存入hbase
      .saveAsHadoopDataset(jobConf)
  }
}
