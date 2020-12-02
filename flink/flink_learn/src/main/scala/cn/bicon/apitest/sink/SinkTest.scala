package cn.bicon.apitest.sink

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object SinkTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputstream = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\flinkLearn\\src\\main\\resources\\sensor.txt")

    val outstream = inputstream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim().toInt, dataArray(2).trim.toDouble).toString //经sensorReading toString方便序列化
    })
    //sink kafka
    outstream.addSink(new FlinkKafkaProducer011[String]("bd134:19092", "finkSinkKafka", new SimpleStringSchema()))

    env.execute("sink_kafka")

  }
}
