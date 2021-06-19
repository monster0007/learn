
package cn.bicon.sparkCore.visit

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
  *         优化
  *         1.userVisitRDD 多次使用可以 使用 缓存优化
  *         2.改变结构 (品类,(点击,下单,支付))
  *         直接用 reduceBykey 优化
  **/
object HotCategory10SessionAnalysis01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("adtotal")
    val sc = new SparkContext(sparkConf)

    val userVisitRDD = sc.textFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\spark\\spark-prictice\\src\\main\\resources\\user_visit_action.txt")
    val top10Rdd = categoryTop10(userVisitRDD)



  }

  def categoryTop10(userVisitRDD: RDD[String]) ={
    //1.统计 (品类,点击次数)
    //一次下单多个商品 13,5,11
    val clickRDD = userVisitRDD.filter(acticon => {
      val datas = acticon.split("_")
      datas(6) != "-1"
    })
    val clickCountRDD = clickRDD.map(action => {
      val datas = action.split("_")
      (datas(6), 1)
    }).reduceByKey(_ + _)

    //2.统计 (品类,下单次数)
    val orderRDD = userVisitRDD.filter(acticon => {
      val datas = acticon.split("_")
      datas(8) != "null"
    })


    val orderMapRDD = orderRDD.map(action => {
      val datas = action.split("_")
      datas(8)
    })

    val orderCountRDD = orderMapRDD.flatMap(action => {
      val cids = action.split(",")
      cids.map(id => (id,1))
    }).reduceByKey(_+_)

    //3.统计 (品类,支付次数)
    val payRDD = userVisitRDD.filter(acticon => {
      val datas = acticon.split("_")
      datas(10) != "null"
    })


    val payMapRDD = payRDD.map(action => {
      val datas = action.split("_")
      datas(10)
    })

    val payCountRDD = payMapRDD.flatMap(action => {
      val cids = action.split(",")
      cids.map(id => (id,1))
    }).reduceByKey(_+_)

    //(品类,(点击,0,0))
    val rdd1 = clickCountRDD.map {
      case (k, v) => {
        (k, (v, 0, 0))
      }
    }
    //(品类,(0,下单,0))
    val rdd2 = orderCountRDD.map {
      case (k, v) => {
        (k, (0, v, 0))
      }
    }

    //(品类,(0,0,支付))
    val rdd3 = payCountRDD.map {
      case (k, v) => {
        (k, (0, 0, v))
      }
    }

    val analysiRDD = rdd1.union(rdd2).union(rdd3).reduceByKey {
      case (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    }

    //4.将统计结果进行分类 去top10
    //(品类,(点击,下单,支付))
    val top10 = analysiRDD.sortBy(_._2._1,false).take(10)

  }
}
