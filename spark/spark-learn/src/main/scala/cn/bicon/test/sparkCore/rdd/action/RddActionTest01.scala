package cn.bicon.test.sparkCore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-24 09:00
  **/
object RddActionTest01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.makeRDD(List(1,2,3,4))
    //collect将数据采集到 Driver 端进行处理
    //rdd.collect()
    //
    rdd.reduce(_+_)
    rdd.count()
    rdd.first()
    rdd.take(3)
    rdd.takeOrdered(3)(Ordering.Int.reverse)
  }

}
