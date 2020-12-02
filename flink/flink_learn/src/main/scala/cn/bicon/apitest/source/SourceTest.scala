package cn.bicon.apitest.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
 * 温度传感器
 */
case  class SensorReading(id : String, timestamp : Long, temperature : BigDecimal)

object SourceTest{
  def main(args: Array[String]): Unit = {
    val env =  StreamExecutionEnvironment.getExecutionEnvironment

   //1.从自定义的集合中读取数据
   val stream1 = env.fromCollection(
     List(
       SensorReading("sensor_1", 1547718199, 35.80018327300259),
       SensorReading("sensor_6", 1547718201, 15.402984393403084),
       SensorReading("sensor_7", 1547718202, 6.720945201171228),
       SensorReading("sensor_10", 1547718205, 38.101067604893444)
     )
    )
    stream1.print("stream1")

    //1.2 fromElements
    //env.fromElements(1,2,3,"string").print()

    //2.从文件中读取数据
    val stream2 = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_learn\\src\\main\\resources\\sensor.txt")

    stream2.print("stream2").setParallelism(1)

    //3.从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "bd134:19092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    stream3.print("stream3").setParallelism(1)

    //4.自定义source
    val stream4 = env.addSource(new SensorSource())
    stream4.print("stream4").setParallelism(1)

    //execute
    env.execute("source test")

  }
}


class SensorSource extends SourceFunction[SensorReading]{
  //运行状态
  private var running = true

  //生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    var curTemp = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )

    while (running){
      //更新温度值
      curTemp =  curTemp.map(
        t => (t._1,t._2+ rand.nextGaussian())
      )

      //获取时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t =>sourceContext.collect( SensorReading(t._1,curTime,t._2))
      )
    }
  }

  //关闭
  override def cancel(): Unit = {
    running = false
  }
}

