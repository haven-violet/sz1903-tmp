package com.tags

/**
  * @Author liaojincheng
  * @Date 2020/6/18 17:14
  * @Version 1.0
  * @Description
  *             定义一个特质（接口）Tags,进行打标签
  */
trait Tags {
  def makeTags(args: Any*): List[(String, Int)]
}
