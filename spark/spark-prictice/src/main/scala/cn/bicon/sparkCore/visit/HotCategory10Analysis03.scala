package cn.bicon.sparkCore.visit

import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-24 15:49
  *         6.1.1 需求说明
  *          品类是指产品的分类，大型电商网站品类分多级，咱们的项目中品类只有一级，不同的
  *          公司可能对热门的定义不一样。我们按照每个品类的点击、下单、支付的量来统计热门品类。
  *          鞋 点击数 下单数 支付数
  *          衣服 点击数 下单数 支付数
  *
  *         HotCategory10Analysis02 shuffle 阶段太多影响性能
  *         优化
  *         点击(品类,(1,0,0))
  *         下单(品类,(0,1,0))
  *         支付(品类,(0,0,1))
  *         直接聚合
  **/
object HotCategory10Analysis03 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("adtotal")
    val sc = new SparkContext(sparkConf)

    val userVisitRDD = sc.textFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\spark\\spark-prictice\\src\\main\\resources\\user_visit_action.txt")
    var flaMapRDD = userVisitRDD.flatMap(
      action => {
      val datas = action.split("_")
      if(datas(6) != "-1"){//点击
        List((datas(6),(1,0,0)))
      }else if(datas(8) != "null") {//下单
        val cids = datas(8).split(",")
        cids.map(id => (id, (0, 1, 0)))
      }else if(datas(10) != "null"){
        val cids = datas(10).split(",")
        cids.map(id => (id, (0, 0, 1)))
      }else{
        Nil
      }
    }).foreach(println)

   /* val analysiRDD = flaMapRDD.reduceByKey{
      case (t1,t2) =>{
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    }*/
    //4.将统计结果进行分类 去top10
    //(品类,(点击,下单,支付))

    //analysiRDD.sortBy(_._2._1,false).take(10).foreach(println)
  }

}
