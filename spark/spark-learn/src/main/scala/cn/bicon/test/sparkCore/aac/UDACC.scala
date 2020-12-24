package cn.bicon.test.sparkCore.aac

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-24 14:09
  *         自定义累加器实现word count
  **/
object UDACC {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List("a","b","c","d","a"))
    val macc = new MyAcc()
    sc.register(macc)

    rdd.foreach(row => macc.add(row))
    println(macc.value)

  }

  class MyAcc() extends AccumulatorV2[String,mutable.Map[String,Int]]{
    private var resMap = mutable.Map[String, Int]()

    //判断累加器是否为初始值
    override def isZero: Boolean = resMap.isEmpty

    //复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new MyAcc()

    //重置累加器
    override def reset(): Unit = resMap.clear()

    //累加
    override def add(word: String): Unit = {
      var newMap= resMap.getOrElse(word,0) + 1
      resMap.update(word,newMap)
    }
    //Driver 端合并累加器(在driver端合并两个map)
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      val map1 = this.resMap
      val map2 = other.value
     /* map2.foreach(m =>{
        println(m)
        var newmap = map1.getOrElse(m._1,0) + 1
        map1.update(m._1,newmap)
      })*/
      map2.foreach{
        case (word,count) => {
          val ncnt = map1.getOrElse(word,0) + count
          map1.update(word,ncnt)
        }
      }
    }

    //获取 累加结果数据
    override def value: mutable.Map[String, Int] = resMap
  }

}
