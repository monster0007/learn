package cn.bicon.orderpay

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-07 17:08
  **/
//input case
case class OrderEvent(orderId: Long,orderType: String,payId: String,eventTime: Long)
//view case
case class OrderRsult(orderId: Long,result: String)

object OrderPayDetect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resources = getClass.getResource("/OrderLog.csv")

    //source
    val streamData = env.readTextFile(resources.getPath)

    //transform
    val dataStream = streamData.map(line => {
      val dataArry = line.split(",")
      OrderEvent(dataArry(0).trim.toLong, dataArry(1).trim, dataArry(2).trim, dataArry(3).trim.toLong)
    }).assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)


    //pattern
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.orderType == "create")
      .followedBy("next").where(_.orderType == "pay")
      .within(Time.minutes(5))

    val resOut = new OutputTag[OrderRsult]("res")

    var patternStream = CEP.pattern(dataStream, orderPayPattern)
      .select( resOut,new OrderTimeOutSelect(),new OrderSelect())

    patternStream.print("order")
    patternStream.getSideOutput[OrderRsult](new OutputTag[OrderRsult]("res")).print("time out")

    env.execute("order pay ")

  }
}

/**
  * 处理超时
  */
class OrderTimeOutSelect() extends PatternTimeoutFunction[OrderEvent,OrderRsult]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderRsult = {
    val begin = pattern.get("begin").iterator().next()
    OrderRsult(begin.orderId,"order time out")
  }
}

class OrderSelect() extends PatternSelectFunction[OrderEvent,OrderRsult]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderRsult = {
    val begin = pattern.get("begin").iterator().next()
    val next = pattern.get("next").iterator().next()
    OrderRsult(begin.orderId,"order pay successfully")
  }
}