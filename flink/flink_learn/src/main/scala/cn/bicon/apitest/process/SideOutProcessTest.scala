package cn.bicon.apitest.process

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 案例:测流输出 自定义process
  * 实现 steam split 分流
  */
object SideOutProcessTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //source
    val dataStream = env.socketTextStream("bd134",6666)
    //transform
    val outStream = dataStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toInt, dataArray(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {//乱序水印延迟1s
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 10000L
    })

    //自定义process 分流器
    val sideStream = outStream.process(new FreezingAlert())
  /*  outStream.split(data =>  {
      if(data.temperature> 30) Seq("High") else Seq("low")
    })*/

    //获取分流器
    sideStream.getSideOutput[String](new OutputTag[String]("freezingAlter")).print("side_out")

    env.execute("side stream")

  }
}

class FreezingAlert() extends ProcessFunction[SensorReading,SensorReading]{
  //分流器输出类型 和id的设定
  lazy val sideOute : OutputTag[String] =  new OutputTag[String]("freezingAlter")

  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if(i.temperature < 32) {//小于0 输出到测流
      //分流器
      context.output(sideOute,s"FreesignAlter ${i.id}")
    }else{
      //主进程
      out.collect(i)
    }
  }
}