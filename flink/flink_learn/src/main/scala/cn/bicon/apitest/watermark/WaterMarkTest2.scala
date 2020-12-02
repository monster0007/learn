package cn.bicon.apitest.watermark

import cn.bicon.apitest.source.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark

object WaterMarkTest2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从调用开始给每个 env 创建一个stream追加时间特性
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置5秒产生一个 watermark
    env.getConfig.setAutoWatermarkInterval(5000)

    env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks( new PeriodicAssigner)

    env.execute("w2")


  }

}


/**
  *  周期是 200 毫秒
  */
class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading]{

   var bound : Long =  60 * 1000  //延时 1分钟

  var maxTs : Long = Long.MinValue // 观察最大时间


  //当前水印时间
  override def getCurrentWatermark: Watermark = {
    new Watermark(bound - maxTs)
  }

  //设置水印时间
  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTs = maxTs.max(t.timestamp)
    t.timestamp
  }
}


class  PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorReading]{
  // b
  val bound = 60 * 1000L


  override def checkAndGetNextWatermark(t: SensorReading, extTs: Long): Watermark = {
    if(t.id == "sensor_1"){
      new Watermark(extTs - bound)
    }else {
      null
    }
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
           t.timestamp
  }
}


