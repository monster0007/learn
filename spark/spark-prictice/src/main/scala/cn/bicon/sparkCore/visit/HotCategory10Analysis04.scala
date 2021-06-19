package cn.bicon.sparkCore.visit

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


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
  *         HotCategory10Analysis03 shuffle reduceByKey 阶段存在shuffle
  *         需要优化,采用 累加器优化shuffle
  *
  *         点击(品类,(1,0,0))
  *         下单(品类,(0,1,0))
  *         支付(品类,(0,0,1))
  *         直接聚合
  **/

object HotCategory10Analysis04 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("adtotal")
    val sc = new SparkContext(sparkConf)

    val userVisitRDD = sc.textFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\spark\\spark-prictice\\src\\main\\resources\\user_visit_action.txt")
    val acc = new HotCategoryAcc()
    sc.register(acc,"HotCategory")
    var foreachRDD = userVisitRDD.foreach(
      action => {
      val datas = action.split("_")
      if(datas(6) != "-1"){//点击
        acc.add(datas(6),"click")
      }else if(datas(8) != "null") {//下单
        val cids = datas(8).split(",")
        cids.foreach(id =>
        {
          acc.add((id,"order"))
        })
      }else if(datas(10) != "null"){
        val cids = datas(10).split(",")
        cids.foreach(id =>
        {
          acc.add((id,"pay"))
        })
      }
    })

    val accVal = acc.value

    val categories = accVal.map({
      case (k,v) => (k,(v.clickCount,v.orderCount,v.payCount))
    })
    //4.将统计结果进行分类 去top10
    //(品类,(点击,下单,支付))

    //categories.toList.sortBy(_._1)(Ordering.Int.reverse).take(10).foreach(println)
    categories.toList.sortWith({
      case (v1,v2) =>{
        if(v1._2._1 > v2._2._1) {
          false
        }else if(v1._2._1 > v2._2._1){
          false
        }else{
          false
        }
      }
    }).take(10).foreach(println)


  }

  case class HotCategory(cid: String,var clickCount: Int, var orderCount: Int, var payCount: Int)
  /***
    * 自定义累加器
    * IN : (品类ID,行为类型)
    * OUT : mutable.Map[String,HotCategory]
    */
  class HotCategoryAcc() extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{
    private var hcMap = mutable.Map[String,HotCategory]()

    override def isZero: Boolean = hcMap.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new HotCategoryAcc()

    override def reset(): Unit = hcMap.clear()

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val category = hcMap.getOrElse(cid,HotCategory(cid,0,0,0))
      if("click" == actionType){
        category.clickCount += 1
      }else if("order" == actionType){
        category.orderCount += 1
      }else if("pay" == actionType){
        category.payCount += 1
      }
      this.hcMap.update(cid,category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      var map1 = this.hcMap
      var map2 = other.value
      map2.foreach{
        case(k ,v) =>{
          val m1 = map1.getOrElse(k,HotCategory(k,0,0,0))
          m1.clickCount += v.clickCount
          m1.orderCount += v.orderCount
          m1.payCount += v.payCount
          map1.update(k,m1)
        }
      }
    }
    override def value: mutable.Map[String, HotCategory] =  hcMap

  }

}
//结果样例类
