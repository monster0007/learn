package cn.bicon.test.sparkCore.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-11 11:23
  **/
object RDD_flatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("distinct")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1,2,3,4,5,6))
    val ids = List(1,2,3,4,5,6)
    println(ids.zip(ids.tail))
  }

}
