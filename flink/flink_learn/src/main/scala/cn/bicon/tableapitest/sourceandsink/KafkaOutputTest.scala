package cn.bicon.tableapitest.sourceandsink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}// flink 1.11.1
//import org.apache.flink.table.api.scala._// flink 1.10.0

/**
 * @Author: shiyu
 * @Date: 2020/12/16 0016 17:16
 */
object KafkaOutputTest {
  def main(args: Array[String]): Unit = {
    //1 创建流失处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //1 表创建流失处理执行环境
    val tableEnv = StreamTableEnvironment.create(env)
    val filePath ="D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_learn\\src\\main\\resources\\"


    val stremFromFile = env.readTextFile(filePath + "sensor.txt")

    //2.source
    val sourceDataStream = stremFromFile.map(data => {
      val dataArray = data.split(",")
      SensorReadingfs(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    //3.转换为表 import org.apache.flink.table.api._
    val table: Table = tableEnv.fromDataStream(sourceDataStream, 'id, 'timestamp as 'ts, 'temperature as 'temp)//字段名
    //4. transform
    val tableRes = table
      .select('id, 'temp)
      //.filter('id === "sensor_1")

    //4.2 transform
    val aggRes = table
      .groupBy('id)
      .select('id, 'id.count as 'ct)

    //5. sink kafka
    tableEnv.connect(new Kafka()
      .version("0.11")//0.9 0.10 0.11+ 三个版本(版本写错可能会有数据类型转换异常!!!)
      .topic("sink_kafka6")
      .property("zookeeper.connect", "bd134:12181")
      .property("bootstrap.servers", "bd134:19092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temp",DataTypes.DOUBLE())
      )
      .createTemporaryTable("KafkaOutTable")

    //1.11 executeInsert / 1.10.0 使用 insertInto
    tableRes.executeInsert("KafkaOutTable")

    //6 execute
    env.execute("table connect output to kafka")

  }
}
