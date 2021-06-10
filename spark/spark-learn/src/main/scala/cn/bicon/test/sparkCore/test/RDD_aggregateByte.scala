package cn.bicon.test.sparkCore.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-10 09:46
  **/
object RDD_aggregateByte {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("distinct")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(("a",1),("b",1),("c",2)))

    val value = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    value.mapValues{
      case(num,cnt) =>{
        num/cnt
      }
    }






    sc.stop()


  }

}
