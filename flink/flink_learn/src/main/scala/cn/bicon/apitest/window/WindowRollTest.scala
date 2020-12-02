package cn.bicon.apitest.window

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * 滚动窗口
  */
object WindowRollTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //设置 EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 默认200ms
    env.getConfig.setAutoWatermarkInterval(100)


    //source
    // val stream = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_learn\\src\\main\\resources\\sensor.txt")
    var stream = env.socketTextStream("bd134",6666)
    stream.print("input")
    // transform
    val dataStream = stream.map(data => {
      val dataArry = data.split(",")
      SensorReading(dataArry(0).trim, dataArry(1).trim.toLong, dataArry(2).trim.toDouble)
    })
      //.assignAscendingTimestamps(_.timestamp * 1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
    })


    val minTempPerWindowStream = dataStream.map(data => {
      (data.id, data.temperature)
    })
      .keyBy(_._1)
      .timeWindow(Time.seconds(5)) //开窗时间 滚动窗口
      //.timeWindow(Time.seconds(15),Time.seconds(5)) //开窗时间 滑动窗口
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))
    minTempPerWindowStream.print("min")

    //execute
    env.execute("window-test")
  }

}


