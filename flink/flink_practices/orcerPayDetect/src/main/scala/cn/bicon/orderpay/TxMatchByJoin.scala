package cn.bicon.orderpay

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author: shiyu
 * @Date: 2020/12/14 0014 13:28
 * 案例说明:只有符合条件的才会陪保留 不会有测输出流
 * 应用典型案例 烟雾和温度报警器同时连续升高
 */

object TxMatchByJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderRes = getClass.getResource("/OrderLog.csv")

    //source
    val orderSource = env.readTextFile(orderRes.getPath)

    //transform
    val orderDataStream = orderSource.map(line => {
      val dataArry = line.split(",")
      OrderEvent(dataArry(0).trim.toLong, dataArry(1).trim, dataArry(2).trim, dataArry(3).trim.toLong)
    }).assignAscendingTimestamps(_.eventTime * 1000L)
      .filter(_.payId !=  "")
      .keyBy(_.payId)

    val receiptRes = getClass.getResource("/ReceiptLog.csv")

    //source
    val receiptSource = env.readTextFile(receiptRes.getPath)

    val receiptDataStream = receiptSource.map(line => {
      val dataArry = line.split(",")
      ReceiptEvent(dataArry(0).trim, dataArry(1).trim, dataArry(2).trim.toLong)
    }).assignAscendingTimestamps(_.eventTime * 1000L)
      .filter(_.txId !=  "")
      .keyBy(_.txId)

    val joinStream = orderDataStream.intervalJoin(receiptDataStream)
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new joinStream())

    joinStream.print()

    env.execute("intervalJoin")

  }
}

class joinStream() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left,right))
  }
}
