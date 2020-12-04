package cn.bicon

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-04 10:12
  * 统计App 营销策略,某个窗口的开始和结束期间的统计结果
  **/

//输入数据
case class AppUserBehavior(userId: String,behavior: String,channel: String,timestamp: Long)
//输出结果
case class AppViewCount(windowStart: String,windowEnd: String,behavior: String,channel: String,count: Long)

object AppMarketingByChannel {
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
        ((line.behavior, line.channel), 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new WindowRest())

    sinkStream.print("appview")


    env.execute("AppViewCount")

  }
}

/**
  * 模拟App数据
  */
class SimulateDataSource() extends RichSourceFunction[AppUserBehavior]{
  //定义运行状态
  var running = true
  val behaviors = Seq("CLICK","DOWNLOAD","INSTALL","UNINSTALL")
  val channels = Seq("weibo","huaweiStore","xiaomiStore","tencentStore")

  override def run(ctx: SourceFunction.SourceContext[AppUserBehavior]): Unit = {
    val valueSize = Long.MaxValue
    var count = 0L

    while (running && (count < valueSize)) {
      val userId = UUID.randomUUID().toString
      val behavior = behaviors(Random.nextInt(behaviors.size))
      val channel = channels(Random.nextInt(channels.size))
      val timestamp  = System.currentTimeMillis()
      ctx.collect(AppUserBehavior(userId,behavior,channel,timestamp))
      count +1
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

class WindowRest() extends ProcessWindowFunction[((String,String),Long),AppViewCount,(String,String),TimeWindow]{
  override def process(key: (String, String), ctx: Context, elements: Iterable[((String, String), Long)], out: Collector[AppViewCount]): Unit = {
    val windowStart = new Timestamp(ctx.window.getStart).toString
    val windowEnd = new Timestamp(ctx.window.getEnd).toString
    val bahavior = key._1
    val channel = key._2
    out.collect(AppViewCount(windowStart,windowEnd,bahavior,channel,elements.size))
  }
}
