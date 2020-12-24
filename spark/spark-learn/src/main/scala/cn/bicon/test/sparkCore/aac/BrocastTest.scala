package cn.bicon.test.sparkCore.aac

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-24 15:09
  **/
object BrocastTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(List(("a",1),("b",1),("c",1)))
    val rdd2 = sc.makeRDD(List(("a",2),("b",2),("c",2)))

    //rdd1.join(rdd2).foreach(println)

    //如果map 数量很大 每个task 都分配一份 产生数据冗余
    //因此需要把数据共享使用 broadcast
    val map = mutable.Map(("a",2),("b",2),("c",2))

    rdd1.map{
      case (k,v) =>{
        (k,(v,map.getOrElse(k,0)))
      }
    }.foreach(println)

    val brmap = sc.broadcast(map)

    rdd1.map{
      case (k,v) =>{
        (k,(v,brmap.value.getOrElse(k,0)))
      }
    }


  }

}
