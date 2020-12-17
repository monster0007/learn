package cn.bicon.tableapitest

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
//import org.apache.flink.table.api.scala._//flink 1.10.0
import org.apache.flink.table.api.bridge.scala._//flink 1.11.1import org.apache.flink.table.descriptors.Json
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * @Author: shiyu
 * @Date: 2020/12/14 0014 17:19
 */
object TableApiTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //1 创建流失处理执行环境
    val tableEnv = StreamTableEnvironment.create(env)

   /* //1.1 创建老版本planner的流失查询
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()//老版本
      .inStreamingMode()//流处理
      .build()

    val oldStreamTableEnv = StreamTableEnvironment.create(env,settings)

    //1.2老版本批处理
    val batchEvn = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEvn = BatchTableEnvironment.create(batchEvn)

    //1.3 blink 版本流处理
    val blinkSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkTableEnv = StreamTableEnvironment.create(env, blinkSettings)

    //1.4 blink 版本批处理
    val blinkBatchSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()

    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)
*/

    //2.连接外部系统读取数据
    val filePath = "D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_learn\\src\\main\\resources\\"

 /*   //注册table 到catelog
    tableEnv.connect(new FileSystem().path(filePath + "sensor.txt"))
      //.withFormat(new OldCsv)
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp1",DataTypes.STRING())//timestamp 是关键字
        .field("temperature",DataTypes.STRING())
      )
      .createTemporaryTable("inputTable")
    //获取table
    val inputTable: Table = tableEnv.from("inputTable")

    //table转换为stream 方便输出
    inputTable.toAppendStream[(String,String,String)]
      .print("inputTable")*/

    //2.2 连接kafka
    val inputKafkaTable = tableEnv.connect(
      new Kafka()
        .version("0.11")//0.9 0.10 0.11+ 三个版本(版本写错可能会有数据类型转换异常!!!)
        .topic("sensor")
        .property("zookeeper.connect", "bd134:12181")
        .property("bootstrap.servers", "bd134:19092")
        .startFromEarliest()
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp1", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputKafkaTable")

    //3.1 table api
    val kafkaTable = tableEnv.from("inputKafkaTable")
    kafkaTable.select('id,'temperature)
      .filter('id === "sensor_1")
      .toAppendStream[(String,Double)]
      .print("kafka table api")
    //3.2 tableSql
    tableEnv.sqlQuery("select id,count(id) as cnt  from inputKafkaTable group by id")
      .toRetractStream[(String,Long)]
      .print("table Sql")
    //3.3 聚合
    var aggStream = kafkaTable
      .groupBy('id)
      .select('id,'id.count as 'count)
      .toRetractStream[(String,Double)]

    aggStream.print("aggregate")

    //3.4 流转换为 table 使用 table api
    val table: Table = tableEnv.fromDataStream(kafkaTable,'id,'count)//字段名
    val renameTable: Table = tableEnv.fromDataStream(aggStream,'id as 'myId,'count as 'ct)//字段名重命名

    //3.4 流转换为 临时表
    tableEnv.createTemporaryView("v1",aggStream)
    //3.5 table 转换为临时表
    tableEnv.createTemporaryView("v2",renameTable)

    //execute
    env.execute("table api ")

  }

}
