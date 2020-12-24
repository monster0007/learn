package cn.bicon.test.sparkCore.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-24 10:16
  * 数据依赖分为 对象依赖和数据依赖
  **/
object Spark_RDD_Dep {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\spark\\spark-learn\\data\\1.txt")
    println(rdd.toDebugString)
    println("+++++++++++++++++++++++++")
    val rdd2 = rdd.flatMap(row => row.split(" "))
    println(rdd2.toDebugString)
    println("+++++++++++++++++++++++++")
    val rdd3 = rdd2.map(row => (row,1))
    println(rdd3.toDebugString)
    println("+++++++++++++++++++++++++")
    val rdd4 = rdd3.reduceByKey(_+_)
    println(rdd4.toDebugString)
    println("+++++++++++++++++++++++++")
    val array = rdd4.collect()
    array.foreach(println)

    sc.stop()
  }

}
