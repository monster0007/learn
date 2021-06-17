package cn.bicon.test.sparkCore.test

import org.apache.spark.sql.SparkSession


/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-21 11:43
  **/
object RDDTest{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val spark = SparkSession.builder().appName("test").master("local")
      .config("spark.default.parallelism",5)// 设置并行度
      .getOrCreate()

    val seq = Seq[Int](1, 2, 3, 4)

    spark.sparkContext.parallelize(seq).foreach(println)
    //scheduler.conf.getInt("spark.default.parallelism", totalCores)
    val rdd = spark.sparkContext.makeRDD(seq,4)
    val rdd2 = rdd.repartition(3)
    //print(10/2)
    rdd2.saveAsTextFile("output")

    //println(spark.sparkContext.textFile("",1))
    //spark.close()

  }
}