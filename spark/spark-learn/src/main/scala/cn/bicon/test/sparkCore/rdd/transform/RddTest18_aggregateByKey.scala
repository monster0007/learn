package cn.bicon.test.sparkCore.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-23 14:47
  **/
object RddTest18_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 1), ("b", 2), ("a", 3)), 2)

    //求 每个分区最大的结果求和
    rdd.aggregateByKey(0)((x,y)=> {x.max(y)}  ,(x,y) => x + y)
      .collect()//.foreach(println)
    //求 key的平均值
    //1 求次数和总和
    val rdd2: RDD[(String,(Int,Int))] = rdd.aggregateByKey((0,0))(
      (t,v) =>{
        (t._1 + v,t._2 + 1)
    },(t1,t2) => {
        (t1._1 + t2._1,t1._2 + t2._2)
    })
    // 求平均值
    rdd2.map(row => {
      (row._1,row._2._1/row._2._2)
    }).collect()//.foreach(println)
    //或者
    rdd2.mapValues(row =>{
      (row._1,row._1/row._2)
    }).collect().foreach(println)
    rdd2.mapValues{
      case (num, cnt) => (num,num/cnt)
    }




  }
  def add(x:Int)=(y:Int)=>(z: Int)=>x+y+z
  def z=(x:Int)=>(y:Int)=>(z: Int)=>x+y+z
}


