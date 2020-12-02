package cn.bicon.apitest.watermark

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WaterMarkTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从调用开始给每个 env 创建一个stream追加时间特性
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置5秒产生一个 watermark
    env.getConfig.setAutoWatermarkInterval(5000)

    //source
    val stream = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_learn\\src\\main\\resources\\sensor.txt")
        .map(data => {
          val dataArry = data.split(",")
          SensorReading(dataArry(0).trim,dataArry(1).trim.toLong,dataArry(2).trim.toDouble)
        })

    val waterMarkStream = stream
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
      override def extractTimestamp(t: SensorReading): Long = {
        t.timestamp * 1000
      }
    })
    waterMarkStream.print()

    env.execute("water mark")
  }
}



