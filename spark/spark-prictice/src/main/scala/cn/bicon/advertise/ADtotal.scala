package cn.bicon.advertise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-23 16:46
  * 案例: 统计每个省的top3 广告
  * 分析:
  * 1.筛选有用的数
  * 2.将省份和广告组织成 wordCount形式是 => ((省份,广告),1)
  * 3.统计每种广告在每个省的数量 reduceBykey(_+_) => ((省份,广告),num)
  * 4.将统计结果转换为省份为key  => (省份,(广告,num))
  * 4.将统计好的结果按照省份分组 groupByKey() =>
  * 5.将每个省份的结果排序排序
  **/
case class AgentEvent(timestamp: String, province: Int, city: Int, user: Long, advertise: Long)
case class AdEvent(province: Int,advertise: Long)
case class AdEventView(province: Int,advertise: Long, times: Long)

object ADtotal {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("adtotal")
    val sc = new SparkContext(sparkConf)

    //source
    val soureRdd = sc.textFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\spark\\spark-prictice\\src\\main\\resources\\agent.log")

    // transform  ((省份,广告),次数)
    val trdd1 = soureRdd.map(row => {
      val datas = row.split(" ")
      ((datas(1).toInt, datas(4).toInt), 1)
    }).reduceByKey(_ + _)
    //(省份,(广告,次数))
    val trdd3= trdd1.map(row => {
      (row._1._1, (row._1._2, row._2))
    })
    //按照省份分组
    trdd3
      .groupByKey()//
      .mapValues(
      iter =>{
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
      ).collect().foreach(println)


  }
}
