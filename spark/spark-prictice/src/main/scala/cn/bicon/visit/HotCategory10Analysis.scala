package cn.bicon.visit

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
  **/
object HotCategory10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("adtotal")
    val sc = new SparkContext(sparkConf)

    val userVisitRDD = sc.textFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\spark\\spark-prictice\\src\\main\\resources\\user_visit_action.txt")

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

    //4.将统计结果进行分类 top10
    //(品类,(点击,下单,支付))
    val analysiRDD:RDD[(String,(Iterable[Int],Iterable[Int],Iterable[Int]))]
            = clickCountRDD.cogroup(orderCountRDD,payCountRDD)
        analysiRDD.map{
          case(k,(it1,it2,it3)) => {
            var rs1 =  0
            var rs2 =  0
            var rs3 =  0
            if(it1.iterator.hasNext){
              rs1 = it1.iterator.next()
            }
            if(it2.iterator.hasNext){
              rs2 = it2.iterator.next()
            }
            if(it3.iterator.hasNext){
              rs3 = it3.iterator.next()
            }
            (k,(rs1,rs2,rs3))
          }
        }.sortBy(_._2._1,false).take(10).foreach(println)




  }

}
