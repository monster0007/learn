package cn.bicon.apitest.window

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * process 窗口
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //source
    var stream = env.socketTextStream("bd134",6666)
    stream.print("input")
    // transform
    val dataStream = stream.map(data => {
      val dataArry = data.split(",")
      SensorReading(dataArry(0).trim, dataArry(1).trim.toLong, dataArry(2).trim.toDouble)
    })
    val minTempPerWindowStream = dataStream.map(data => {
      (data.id, data.temperature)
    })
      .keyBy(_._1)
      .timeWindow(Time
        .seconds(5)) //开窗时间 precess 时间 滚动窗口
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))
    minTempPerWindowStream.print("min")




    //execute
    env.execute("window-test")

  }
}

class WindowTest {
}
