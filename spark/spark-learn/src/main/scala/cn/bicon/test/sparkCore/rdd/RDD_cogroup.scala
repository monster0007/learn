package cn.bicon.test.sparkCore.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-10 13:55
  **/
object RDD_cogroup {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 1), ("c", 1), ("d", 1)))
    val rdd2 = sc.makeRDD(List(("a", 1), ("b", 1), ("c", 1)))
    //rdd1.cogroup(rdd2).foreach(println)

    val rdd3 = sc.makeRDD(List(1, 2, 3, 4), 4)
    println(rdd3.getNumPartitions)
    val i = rdd3.aggregate(10)(_ + _ , _ + _)
    println(i)


  }

}
