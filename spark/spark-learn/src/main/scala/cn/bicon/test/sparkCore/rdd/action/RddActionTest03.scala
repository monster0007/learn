package cn.bicon.test.sparkCore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-24 09:00
  **/
object RddActionTest03 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.makeRDD(List(1,2,3,4),2)
    // action
    rdd.aggregate(0)(_+_,_+_)
    //13 + 17 = 30
    //aggregateBykey() 初始值只参加分区内计算
    // 10 +17 +13 = 40
    //aggregate() 初始值参加分区内 和 分区间
    rdd.aggregate(10)(_+_,_+_)
    rdd.fold(0)(_+_)
  }

}
