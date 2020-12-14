package cn.bicon.orderpay

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author: shiyu
 * @Date: 2020/12/14 0014 09:48
 * 案例:支付订单交易对账
 * 1.订单数据源支付信息数据
 * 2.第三方支付接口返回信息
 * 3.将 1 2 两个数据源使用 connect 合并
 * 4.使用coMap + 状态编程处理数据
 * 重新CoMap
 * 4.1 定义两个未匹配上的输出流unmatchPay,unmatchrecepit
 * 4.2 element1 (订单)处理
 * 获取收款状态如果 不为空 则对账完毕 清空 收款 状态
 * 如果收款状态为空,还未匹配上 更新付款订单状态 并设置定时器 延时5秒 (如果超时则在触发器 将数据发送到侧输出流)
 * 4.3 element2(收款)处理
 * 获取 付款订单状态,如果不为空 则对账完毕,发送数据到主流并清空付款状态
 * 如果付款订单为空 表示还未匹配上,更新收款订单状态, 并设置定时器 延时5秒 (如果超时则在触发器 将数据发送到侧输出流)
 *
 * 注意:
 * 1.两个不同的数据流 watermartk 已最小的那个为准
 */

//收款单
case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )

object TxMatchDetect {

  val unmatchedpays = new OutputTag[OrderEvent]("unmatchedpays")
  val unmatchedreceipts = new OutputTag[ReceiptEvent]("unmatchedreceipts")

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

    //connect
    val connectStream = orderDataStream.connect(receiptDataStream)
    //CoMap
    val coMapStream = connectStream.process(new TxMatchMap())

    coMapStream.print("coMapStream")

    coMapStream.getSideOutput(unmatchedpays).print("unmatchedpays")
    coMapStream.getSideOutput(unmatchedreceipts).print("unmatchedreceipts")



    env.execute("TxMatchDetect")


  }
  class TxMatchMap() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{

    //pay状态值
    lazy val payState: ValueState[OrderEvent]  =  getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state",classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent]  =  getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state",classOf[ReceiptEvent]))



    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //获取receipt
      val receipt = receiptState.value()
      //如果receipt不为空 则匹配上
      if(receipt != null){
        out.collect(pay,receipt)
        //清空 receipt状态
        receiptState.clear()
      }else{//如果为null 没有匹配上 这是定时器 超时则输出到测输出流
        //更新pay
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)//在事件时间的基础上延迟5秒
      }
    }

    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // pay
      val pay = payState.value()
      //匹配上 输出到主流
      if(pay != null) {
        out.collect((pay,receipt))
        payState.clear()
      }else{
        //更新 receipt
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000L )//没有匹配是上则设置定时器 5秒后延迟
      }
    }


    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val pay = payState.value()
      val receipt = receiptState.value()
      // 如果 pay 不为空 则没有匹配上
      if(pay != null) {
        ctx.output(unmatchedpays,pay)
        payState.clear()
      }

      //如果 receipt 不为空 则没有匹配上
      if(receipt != null) {
        ctx.output(unmatchedreceipts,receipt)
        receiptState.clear()
      }

    }
  }
}


