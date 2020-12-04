package cn.bicon

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-03 14:13
  **/

/**
  * 实时统计海量数据,使用bloom 过滤器
  * 每来一条数据触发一次窗口
  */
object UvwithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //env.setParallelism(1)
    val dataStream = env.readTextFile(getClass.getResource("/UserBehavior.csv").getPath)
   var sinkSrem = dataStream .map(data => {
      val dataArray = data.split(",")
      UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
    })
      .filter(_.behavior == "pv")//只统计pv
      .assignAscendingTimestamps(_.timestamp * 1000L) //数据源已按照时间戳排序
      .map(data => ("dummyKey",data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())// 自定义窗口触发规则
      .process(new UvCountWithBloom())// 自定义窗口处理规则

    sinkSrem.print("bloom")

    env.execute("bloom process")

  }
}

/**
  * 触发窗口
  * 每条数据都触发一个窗口
  */
class MyTrigger() extends Trigger[(String,Long),TimeWindow]{
  //每条数据都触发一个窗口,所以跳过数据处理
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
  //每条数据都触发一个窗口,所以跳过数据处理
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
  //每条数据都 自动清空 所以不用操作
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  //每条数据都触操作
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //每条数据都触操作 并清空
    TriggerResult.FIRE_AND_PURGE
  }
}
//自定义一个布隆过滤器
class Bloom(size: Long) extends Serializable{
  //位图总大小 2^28  16M
  private  val cap = if (size > 0) size else 1 << 27

  def hash(value: String,seed: Int): Long ={
    var result = 0L
    for(i <- 0 until value.length){
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }
}

/**
  * 处理数据
  */
class UvCountWithBloom() extends ProcessWindowFunction[(String,Long),UviewCount,String,TimeWindow]{
  //创建 redis 连接
  lazy val jedis = new Jedis("bd137",27000)
  //创建bloom 过滤器 64M
  lazy val bloom = new Bloom(1 << 29)

  //主要修改 位图 和 count 数量
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UviewCount]): Unit = {
    jedis.auth("bicon.redis.test@123456")

    //位图的存储方式 key是windowEnd value 是bitmap
    val storedKey = context .window.getEnd.toString
    //计数器
    var count = 0L
    //把每个窗口的uv count值存入名为count的表中,内容为(windowEd -> uvcount),所以要先读取redis
    if(jedis.hget("count",storedKey) != null) {
      count = jedis.hget("count",storedKey).toLong
    }
    //userid
    val userId = elements.last._2.toString
    //userid 在位图中的offset
    val offset = bloom.hash(userId, 61)
    //判断 offset 是否存在位图中
    val isExist = jedis.getbit(storedKey,offset)
    if(!isExist){//如果不存在
      //将位图更新为 1
      jedis.setbit(storedKey,offset,true)
      //将数据 加1 并存入redis
      jedis.hset("count",storedKey,(count + 1 ).toString)
      out.collect(UviewCount(storedKey.toLong,count + 1))
    }else{//如果存在不操作reids
      out.collect(UviewCount(storedKey.toLong,count))
    }
  }
}