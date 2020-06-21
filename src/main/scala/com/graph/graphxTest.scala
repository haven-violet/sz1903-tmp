package com.graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * @Author liaojincheng
  * @Date 2020/6/21 21:04
  * @Version 1.0
  * @Description
  */
object graphxTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("graph")
      .master("local")
      .getOrCreate()
    //构造点
    val rdd = spark.sparkContext.makeRDD(Seq(
      (1L, ("张三", 23)),
      (2L, ("李四", 56)),
      (6L, ("王五", 28)),
      (9L, ("赵六", 24)),
      (16L, ("小明", 53)),
      (21L, ("小红", 33)),
      (44L, ("小黄", 25)),
      (5L, ("白胡子", 72)),
      (7L, ("索隆", 25)),
      (133L, ("田七", 13)),
      (158L, ("路飞", 20)),
      (138L, ("小绿", 23))
    ))

    //构造边
    val rdd2 = spark.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    //构建图
    val graph: Graph[(String, Int), Int] = Graph(rdd, rdd2)
//    graph.vertices.foreach(println)
//    graph.edges.foreach(println)
//    graph.connectedComponents().vertices.foreach(println)
//    println()
//    graph.connectedComponents().edges.foreach(println)
/*
(21,1)
(16,1)
(158,5)
(138,1)
(133,1)
(1,1)
(6,1)
(7,5)
(9,1)
(44,1)
(5,5)
(2,1)

Edge(1,133,0)
Edge(2,133,0)
Edge(5,158,0)
Edge(6,133,0)
Edge(6,138,0)
Edge(7,158,0)
Edge(9,133,0)
Edge(16,138,0)
Edge(21,138,0)
Edge(44,138,0)
 */
    /*
    TODO 在一个关系网中相连的点构成的一个图中选择最小值的一个点做顶点, 其余的点(包括该顶点) 进行关联该顶点 ( 1 to List , 顶点1)
     这样和原来的点 （1 to List, 值）就可以进行join  ==》  (1 to List, (顶点 ,  值))  ==》  map  (顶点, 值)
                                                   ==》 reduceByKey (顶点, List(值))
     */
    //TODO 获取连通图中的顶点ID （最小的点）
    graph.connectedComponents().vertices
      .join(rdd).map({
      case (userId, (cmId, (name, age))) => (cmId, List((name, age)))
    }).reduceByKey(_++_).foreach(println)
  }
}
