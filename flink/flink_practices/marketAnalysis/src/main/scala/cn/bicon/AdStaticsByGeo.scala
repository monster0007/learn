package cn.bicon

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-04 15:38
  *         广告投放量分析 每5分钟限时前1小时的广告点击量
  **/
//投放广告的样例类
case class AdClickLog(userId: String,adId: String,provence: String,city: String,timestamp: Long)
//统计结果样例类
case class AdViewCount(windowEnd: String,provence: String,count: Long)
//黑名单样例类
case class BlackListWarning(userId: String,adId: String,message: String)
object AdStaticsByGeo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val path = getClass.getResource("/AdClickLog.csv")
     println("source :" + path)
    val dataStream = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_practices\\marketAnalysis\\src\\main\\resources\\AdClickLog.csv")

    val adEventStream = dataStream.map(line => {
      val dataArray = line.split(",")
      AdClickLog(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    /**
      * 过滤恶意点击
      * 1.根据userId  adId 统计出点击次数
      * 2.将黑名单结果输出到侧输出流
      *  2.1
      */
    val blackStream = adEventStream
      .keyBy(data => (data.userId, data.adId))
      .process(new BlackListFunction(100))

    blackStream.getSideOutput[BlackListWarning](new OutputTag[BlackListWarning]("blacklist")).print("blacklist")


    //根据省统计广告点击量
    blackStream.keyBy(_.provence)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new AdAggCount(), new AdResultWindow())
       .print("ad statics")

    env.execute("ad statics")



  }

}

class AdAggCount() extends AggregateFunction[AdClickLog,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdResultWindow() extends  WindowFunction[Long,AdViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdViewCount]): Unit = {
    val windowEnd = new Timestamp( window.getEnd).toString
    out.collect(AdViewCount(windowEnd,key,input.iterator.next()))
  }
}

/**
  *
  * @param size
  */
class BlackListFunction(size: Int) extends KeyedProcessFunction[(String,String),AdClickLog,AdClickLog]{
  //count 点击次数
  lazy val countSate: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count",classOf[Long]))
  //时间
  lazy val timestampSate: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timestamp",classOf[Long]))
  //第一次发生黑名单
  lazy val firstSentSate: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("firstSent",classOf[Boolean]))

  //定义一个测输出流
  lazy val blackListOut: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blacklist")

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(String, String), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    //获取count
    val count = countSate.value()
    //获取时间戳
    val timestamp = timestampSate.value()

    //如果时间为零注册时间
    if(count == 0) {
      val ts = (ctx.timerService().currentProcessingTime()/ (1000 * 60 * 60 * 24) + 1 ) * (1000 * 60 * 60 * 24)
      println("ts:" + ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
      timestampSate.update(ts)
    }

    //若 count 大于 size 这输出到测输出流
      if (count >= size) {
        if(!firstSentSate.value()){//如果为false(第一次发生)
          firstSentSate.update(true)
          ctx.output(blackListOut,BlackListWarning(value.userId,value.adId,"今天点击量大于" + count))
        }
      return
    }
    //计数器更新
    countSate.update(count +1)
    //正常输出
    out.collect(value)
  }

  //清空状态
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(String, String), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if (timestamp == timestampSate.value()) {
      firstSentSate.clear()
      countSate.clear()
    }
  }
}
