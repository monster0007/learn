package cn.bicon

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-02 17:28
  *          实时流量统计
  *          我们在这里先实现“热门页面浏览数”的统计，也就是读取服务器日志中的每
  *          一行 log，统计在一段时间内用户访问每一个 url 的次数，然后排序输出显示。
  *          具体做法为：每隔 5 秒，输出最近 10 分钟内访问量最多的前 N 个 URL。可以
  *          看出，这个需求与之前“实时热门商品统计”非常类似，所以我们完全可以借鉴此
  *          前的代码。
  *
  *          技术实现:
  *          step0.需要创建两个样例类
  *           1.source 数据样例类
  *           2.实时热门商品统计结果样例类
  *          step1.source 读取数据
  *          step2.transform 数据
  *              1.将数据包装到样例类 设置watermark(无序)
  *              2.根据url keyedBy
  *              3.开窗timeWindows(10min,5s)
  *              4.设置数据延迟时间allowLaterNess(60s)
  *              5.聚合开窗结果aggregate(new AggCount(),new WindowResult() ) //并将数据穿度到下一个Stream
  *              6.自定义AggCount统计数量
  *              7.自定义WindowResult包装统计结果,至此完成所有窗口内从数据聚合
  *              8.根据窗口结束时间再次做 keyedBy(_.windowEnd) 把相同的窗口数据聚合起来
  *              9.process(new TopNHotItems(3)) ,自定义process 把不同窗口的数据排序(sortBy),取出topN(take)
  *              10.自定义sink 把数据发送出去
  */


//日志样例类
case class ApacheLogEvent(ip: String,userId : String,enventTime : Long,method : String,url : String)

//统计结果样例类  ()
case class UrlViewCount(url: String,windowEnd: Long,count: Long)
object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置watermark 为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //source
    val sourceStream = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_practices\\networkFlowAnalysis\\src\\main\\resources\\apache.log")

    //transform
    val sinkStream = sourceStream.map(data => {
      val dataArray = data.split(" ")
      val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp = sdf.parse(dataArray(3).trim).getTime
      ApacheLogEvent(dataArray(0).trim, dataArray(2).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(t: ApacheLogEvent): Long = t.enventTime
    })
    .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.seconds(60))//数据延迟
        .aggregate(new CountAgg(),new WindowResult())//所有窗口的聚合结果
        .keyBy(_.windowEnd)
        .process(new TopNNetworkFlow(3))

    sinkStream.print("networkflow")



    //execute
    env.execute("networkflow")

  }

}

/***
  * 对输入数据进行聚合
  */
class CountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

/***
  * 将结果包装成 UrlViewCount
  * @param topN
  */
class WindowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}

class TopNNetworkFlow(topN: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
  //listState 用来装list结果
  private var listState: ListState[UrlViewCount] = _

  override def open(parameters: Configuration): Unit = {
    listState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("listState",classOf[UrlViewCount]))
  }

  override def processElement(u: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    listState.add(u)
    ctx.timerService().registerEventTimeTimer(u.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //获取list
    val listBuffer: ListBuffer[UrlViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for(it <- listState.get()) {
      listBuffer += it
    }
    //清空数据
    listState.clear()
    //排序取出topN
    val viewCounts = listBuffer.sortBy(_.count)(Ordering.Long.reverse).take(topN)
    listBuffer.sorted(Ordering.Long.reverse)
    //listBuffer.sortWith(_.count > _.count).take(topN)

    val stringBuilder = new StringBuilder()
    stringBuilder.append("时间: " + new Timestamp(timestamp - 1) + "\n")

    stringBuilder.append("=========================\n")
    for (i <- viewCounts.indices){
      stringBuilder
        .append("No "+ (i+1) + "\t")
        .append("Url " + viewCounts.get(i).url + "\t")
        .append("点击量 " + listBuffer.get(i).count + "\n")
    }
    stringBuilder.append("=========================\n")
    out.collect(stringBuilder.toString())
  }
}

