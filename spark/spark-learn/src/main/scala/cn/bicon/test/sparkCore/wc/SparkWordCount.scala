package cn.bicon.test.sparkCore.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-24 09:23
  **/
object SparkWordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("wc")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\spark\\spark-learn\\data\\1.txt")
    val res = rdd.flatMap(_.split(" "))//将所有元素拉平到同一个rdd
      .map((_, 1))//将数据转换为 (word,1) 的元祖
      .reduceByKey(_ + _)//按照key聚合
    res.foreach(println)

    sc.stop()
  println( 8/3)
  println( 7/3)

  }

}
