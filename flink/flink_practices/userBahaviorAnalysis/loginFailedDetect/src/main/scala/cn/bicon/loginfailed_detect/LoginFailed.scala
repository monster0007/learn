package cn.bicon.loginfailed_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-07 13:33
  **/
//输入样例类 LoginEvent
case class LoginEvent(userId: Long,ip: String,eventType: String,eventTime: Long)
//输出样例类
case class Warings(userId: Long,startTime: Long,endTime: Long,EventType: String)
object LoginFailed {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resources = getClass.getResource("/LoginLog.csv")

    val streamData = env.readTextFile(resources.getPath)

    val sinkData = streamData.map(line => {
      val dataArray = line.split(",")
      LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime
      })
      .keyBy(_.userId)
      .process(new LoginFaliedResultFunction(2))
    sinkData.print("login f")


    env.execute("login failed")

  }

}

class LoginFaliedResultFunction(maxNum: Int) extends KeyedProcessFunction[Long,LoginEvent,LoginEvent]{
  lazy val listState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("listState",classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, out: Collector[LoginEvent]): Unit = {
    //如果登录失败将数据添加到 listState
     if (value.eventType == "fail") {
         //第一次失败 设置定时器
         if(!listState.get().iterator().hasNext) {
         ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 2000L)//在当前的事件时间基础上 延迟2秒
         }
       listState.add(value)
     }else{
       listState.clear()
     }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
    //获取 失败的数据
    val allLogins: ListBuffer[LoginEvent] = new ListBuffer()

    import scala.collection.JavaConversions._
    //获取list State 值
    for (login <- listState.get) {
      allLogins += login
    }

    listState.clear()

    if(allLogins.length  >= maxNum){
      out.collect(allLogins.head)
    }


  }
}
