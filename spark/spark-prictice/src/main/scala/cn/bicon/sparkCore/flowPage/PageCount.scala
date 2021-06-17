package cn.bicon.sparkCore.flowPage

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-11 15:34
  **/
object PageCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("adtotal")
    val sc = new SparkContext(sparkConf)
    val userVisitRDD = sc.textFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\spark\\spark-prictice\\src\\main\\resources\\user_visit_action.txt")

    val userVisitMapRDD =  userVisitRDD.map(action =>{
      val datas = action.split("_")
      UserVisitAction(
        datas(0),
        datas(1).toLong,
        datas(2),
        datas(3).toLong,
        datas(4),
        datas(5),
        datas(6).toLong,
        datas(7).toLong,
        datas(8),
        datas(9),
        datas(10),
        datas(11),
        datas(12).toLong
      )
    })

    userVisitMapRDD.cache()
    //1.求分子
    //指定ids页面
    val ids = List(1,2,3,4,5,6,7)
    val okflowIds = ids.zip(ids.tail)

    val pageMap = userVisitMapRDD
        .filter(action =>{
          ids.init.contains(action.page_id)//过滤页面
    })
      .map {
      case (userVisit) => {
        (userVisit.page_id, 1)
      }
    }.reduceByKey(_ + _).collect().toMap


    //2 求分母
    val trdd = userVisitMapRDD.map(visits => {
      (visits.session_id, visits.page_id, visits.action_time)
    })
    val sesSionRdd = trdd.groupBy(_._1)
    val mole = sesSionRdd.mapValues(iter => {
      val sortList = iter.toList.sortBy(_._3)
      val flowIds = sortList.map(_._2)
      val pageIds = flowIds.zip(flowIds.tail)
      pageIds
          .filter(action => {
            okflowIds.contains(action)//过滤页面 (1-2,2-3,3-4,4-5,5-6,6-7)
          })
        .map(t => {
        (t, 1)
      })
    }).map(action => {
      action._2
    }).flatMap(line => line)
      .reduceByKey(_ + _)

    //3.求和
    mole.foreach({
      case ((pageid1,pageid2),sum) =>{
        val lon = pageMap.getOrElse(pageid1,0)
        println(s"页面${pageid1} -> ${pageid2}单跳转换率" + sum.toDouble/lon)
      }
    })


    /*val sessionRdd = userVisitMapRDD.groupBy(_.session_id)

    val value = sessionRdd.mapValues(iter => {
      val sortList: immutable.Seq[UserVisitAction] = iter.toList.sortBy(_.action_time)
      val flowIds = sortList.map(_.page_id)
      val pageflowIds = flowIds.zip(flowIds.tail)
      pageflowIds.map(t => (t, 1))
    })
    val pageIds = value.map(_._2).flatMap(list => list)
      .reduceByKey(_ + _)

    pageIds.foreach{
      case((pageid1,pageid2),sum) =>{
        val long = pageMap.getOrElse(pageid1,0)
        println(s"页面${pageid1} -> ${pageid2}单挑转换率" + sum.toDouble/long)
      }

    }*/

  }

  //3.计算





}

//用户访问动作表
case class UserVisitAction(
                            date: String,//用户点击行为的日期
                            user_id: Long,//用户的 ID
                            session_id: String,//Session 的 ID
                            page_id: Long,//某个页面的 ID
                            action_time: String,//动作的时间点
                            search_keyword: String,//用户搜索的关键词
                            click_category_id: Long,//某一个商品品类的 ID
                            click_product_id: Long,//某一个商品的 ID
                            order_category_ids: String,//一次订单中所有品类的 ID 集合
                            order_product_ids: String,//一次订单中所有商品的 ID 集合
                            pay_category_ids: String,//一次支付中所有品类的 ID 集合
                            pay_product_ids: String,//一次支付中所有商品的 ID 集合
                            city_id: Long
                          )//城市 id
