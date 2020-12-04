package cn.bicon

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-04 11:13
  **/
/**
  * 统计app 行为数据了 1hour 5 滑窗
  */
object AppMarketing {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//时间语义
    val dataStream = env.addSource( new SimulateDataSource())
    //dataStream.print("source")

    val sinkStream = dataStream
      .filter(_.behavior != "UNINSTALL")
      .assignAscendingTimestamps(_.timestamp)
      .map(line => {
        ("dummyKey",1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(5))
        .aggregate(new AggCount(),new AppWindowRest())

    sinkStream.print("appview")


    env.execute("AppViewCount")

  }

}

class AggCount() extends AggregateFunction[(String,Long),Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class AppWindowRest() extends WindowFunction[Long,AppViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AppViewCount]): Unit = {
    val windowStrat = new Timestamp(window.getStart).toString
    val windowEnd = new Timestamp(window.getEnd).toString
    val count = input.iterator.next()
    out.collect(AppViewCount(windowStrat,windowEnd,"app" ,"total",count))
  }
}
