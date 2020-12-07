package cn.bicon.hotitems

import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


//用户行为
case class UserBehavior(userId : Long,itemId : Long,categoryId : Int,behavior : String,timestamp : Long)

//热门商品
case class ItemViewCount(itemId : Long,windowEnd : Long,count : Long)

/**
  * 案例1:实时统计热门商品
  * 实时统计最近1小时的热门商品topN,每5分钟限时一次
  *
  * 技术实现 为什么要用两次keyBy
  * 1.在agreegate 阶段已将将结果封装为 viewCount
  * key理解为 每条数据都有的格式都为viewCount格式
  * 所以要按照viewCount.widowEnd 进行keyBy 聚合
  *
  */
object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置处理时间为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //source
    val dataStream = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_practices\\userBahaviorAnalysis\\hotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //transform
    val behaviorStream = dataStream
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .filter(_.behavior == "pv") //
      .assignAscendingTimestamps(_.timestamp * 1000L) //数据源已按照时间戳排序
      .keyBy(_.itemId)
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult())//按照窗口聚合(12个窗口)
        .keyBy(_.windowEnd)//按照窗口分组
        .process(new TopNHotItems(3))

    behaviorStream.print("behaviorStream")

    //execute
    env.execute("userBehavior")
  }
}

/***
  * 热门商品求和
  */
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  //创建计数器
  override def createAccumulator(): Long = 0L
  //计数
  override def add(in: UserBehavior, acc: Long): Long = acc + 1
  //获取结果
  override def getResult(acc: Long): Long = acc
  //对不同分区的 累加结果 求和
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

/***
  *
  */
class CountAvg() extends AggregateFunction[UserBehavior,(Long,Long),Long]{
  override def createAccumulator(): (Long, Long) = (0L,0L)
  override def add(in: UserBehavior, acc: (Long, Long)): (Long, Long) = (acc._1 + 1,acc._2 + 1)

  override def getResult(acc: (Long, Long)): Long = (acc._1/acc._2)

  override def merge(acc: (Long, Long), acc1: (Long, Long)): (Long, Long) = (acc._1 + acc1._1,acc._2 + acc1._2)
}

/***
  * transform keyby(_.itemId) key 直接返回为long
  * 如果使用 keyby("itemId") key 返回为 Tuple1
  * 需要转化  val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
  */
class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val count = input.iterator.next()
    out.collect(ItemViewCount(key,window.getEnd,count))
  }
}

/***
  * 商品 topN
  * @param topN
  */
class TopNHotItems(topN: Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
  //保证状态数据
  private var listItemsState : ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {
    listItemsState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("listItems",classOf[ItemViewCount]))
  }

  override def processElement(in: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //把数据存入 listState
    listItemsState.add(in)
    //注册定时器
    ctx.timerService().registerEventTimeTimer(in.windowEnd + 1)
  }

  //
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //获取窗口数据
    val items:lang.Iterable[ItemViewCount] = listItemsState.get()
    //所有集合数据
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for(it <- items){
      allItems += it
    }

    //按照count 排序 取出topN
    val resTopN = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topN)

    //输出
    val outStrins = new StringBuilder()
    outStrins.append("====================================\n")
      .append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for(i <- resTopN.indices) {
      //e.g NO1 : 商品ID=12224 浏览量=123
      outStrins.append("No ").append(i + 1)
        .append("商品ID=" + resTopN(i).itemId + "\t")
        .append("浏览量=" + resTopN(i).count).append("\n")
    }
    outStrins.append("====================================\n")

    out.collect(outStrins.toString)
  }
}



