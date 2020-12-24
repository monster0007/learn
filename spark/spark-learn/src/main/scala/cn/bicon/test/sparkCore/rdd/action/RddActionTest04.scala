package cn.bicon.test.sparkCore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-24 09:00
  **/
object RddActionTest04 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.makeRDD(List(1,2,3,4),2)

    rdd.countByValue().foreach(println)
    val rdd2  = sc.makeRDD(List(("a",1),("a",2),("b",1),("c",1)),2)
    rdd2.foreach(println)

  }

}
