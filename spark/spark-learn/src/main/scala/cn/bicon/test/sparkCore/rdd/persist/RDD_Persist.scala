package cn.bicon.test.sparkCore.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-24 10:57
  **/
object RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("./cp")

    //check point 不会删除
    val rdd = sc.makeRDD(List("hello spark hello scala","hello java"))
    //val rdd = sc.textFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\spark\\spark-learn\\data\\1.txt")
    val rdd2 = rdd.flatMap(row => row.split(" "))
    val rdd3 = rdd2.map(row => (row,1))

    val rdd4 = rdd3.reduceByKey(_+_)
    rdd4.checkpoint()
    //rdd4.cache()
    ///rdd4.persist(StorageLevel.DISK_ONLY)
    val array = rdd4.collect()
    array.foreach(println)
  }

}
