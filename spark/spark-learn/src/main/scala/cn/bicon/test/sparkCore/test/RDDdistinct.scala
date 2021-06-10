package cn.bicon.test.sparkCore.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-09 11:15
  **/
object RDDdistinct {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("distinct")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1,2,3,4,5,5,4,3,2,2))
    rdd.distinct().map(x => (x ,null))
      .reduceByKey((x,y) => x)
      .map(_._1)

      .foreach(println)
    sc.stop()
  }
}
