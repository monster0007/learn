package cn.bicon.loginfailed_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-07 15:42
  **/
object LoginFailedWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resources = getClass.getResource("/LoginLog.csv")

    val streamData = env.readTextFile(resources.getPath)

    val loginDataStream = streamData.map(line => {
      val dataArray = line.split(",")
      LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime
      })
      .keyBy(_.userId)

    //定义CEP pattern
    val loginPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail").within(Time.seconds(2))

    //使用CEP 处理输入流
    val patternStream = CEP.pattern(loginDataStream,loginPattern)

    patternStream.select(new PselectFunction()).print("CEP")

    env.execute("cep job")

  }

}



class PselectFunction() extends PatternSelectFunction[LoginEvent,Warings]{
  override def select(pattern: util.Map[String, util.List[LoginEvent]]): Warings = {
    //从map中取出对应的事件
    val firstEvent = pattern.get("begin").iterator().next()
    val lastEvent = pattern.get("next").iterator().next()
    Warings(firstEvent.userId,firstEvent.eventTime,lastEvent.eventTime,lastEvent.eventType)
  }
}
