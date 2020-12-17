package cn.bicon.tableapitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
//import org.apache.flink.table.api.scala._//flink 1.10.0

import org.apache.flink.table.descriptors.FileSystem
import org.apache.flink.table.api.bridge.scala._//flink 1.11.1import org.apache.flink.table.descriptors.Jsonimport org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Csv, Schema}

/**
 * @Author: shiyu
 * @Date: 2020/12/16 0016 15:28
 * 案例:
 * 将数据从文件读取,transform之后再写入新的文件
 */
case  class SensorReadingfs(id : String, timestamp : Long, temperature : Double)


object FsOutputTest {
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
      .filter('id === "sensor_1")

    //4.2 transform
    val aggRes = table
      .groupBy('id)
      .select('id, 'id.count as 'ct)

    //5. sink
    tableEnv.connect(new FileSystem().path( filePath + "out.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temp",DataTypes.DOUBLE())
      ).inAppendMode()
      .createTemporaryTable("outTable")

    tableRes.insertInto("outTable")

    //6 execute
    env.execute("table connect out put")





  }

}
