package cn.bicon

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-03 10:37
  **/


case class UserBehavior(userId : Long,itemId : Long,categoryId : Int,behavior : String,timestamp : Long)


/***
  * 统计所有页面在 1hour 点击量
  */
object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置处理时间为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val path = getClass.getResource("/UserBehavior.csv")

    val dataStream = env.readTextFile(path.getPath)
    dataStream .map(data => {
      val dataArray = data.split(",")
      UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
    })
      .filter(_.behavior == "pv")//只统计pv
      .assignAscendingTimestamps(_.timestamp * 1000L) //数据源已按照时间戳排序
      .map(line => ("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)
      .print("pvcount")

    env.execute("pvCount")

  }
}
