package cn.bicon

import java.sql.{Date, Timestamp}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-03 10:58
  **/
//case class UserBehavior(userId : Long,itemId : Long,categoryId : Int,behavior : String,timestamp : Long)
case class UviewCount(serId : Long,count: Long)

/**
  * 统计一小时的 用户点击量
  */
object UniqueVister {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
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
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountWindow())
      .print("ucount")

    env.execute("u count")
  }
}

class UvCountWindow() extends AllWindowFunction[UserBehavior,UviewCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UviewCount]): Unit = {
    val s: collection.mutable.Set[Long] = collection.mutable.Set()
    var idSet = Set[Long]()
    for(u <- input){
      idSet += u.userId
    }
    //out.collect(UviewCount(new Timestamp(window.getEnd -1),idSet.size))
    out.collect(UviewCount(window.getEnd,idSet.size))
  }
}
