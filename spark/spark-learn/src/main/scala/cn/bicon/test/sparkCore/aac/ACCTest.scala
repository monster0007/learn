package cn.bicon.test.sparkCore.aac

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-24 13:50
  *  累加器:每执行一次 action 算子 触发一次
  *  1.少加 map
  *  2.多加 collect
  **/
object ACCTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    //
    val res = rdd.reduce(_+_)
    println(res)
    var sum = 0
    val sumAcc = sc.longAccumulator("sum")

    rdd.foreach(num  =>{
      sumAcc.add(num)
    })

    println(sumAcc.value)

  }

}
