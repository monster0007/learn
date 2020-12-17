package cn.bicon.tableapitest.sourceandsink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._// flink 1.11.1
//import org.apache.flink.table.api.scala._// flink 1.10.0

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-17 10:07
  **/
object MySQLOutputTest {
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

    //5. sink mysql
    val sinkDDL =
      """
        |create table jdbcOutputTable(
        |id varchar(20) not null,
        |ct bigint not null
        |) with (
        |'connector.type' = 'jdbc',
        |'connector.url' = 'jdbc:mysql://bd208:3306/test',
        | 'connector.table' = 'sinktable',  -- required: jdbc table name
        |'connector.driver' = 'com.mysql.jdbc.Driver',
        |'connector.username' = 'root',
        |'connector.password' = 'bicon@123'
        |)
      """.stripMargin
   tableEnv.sqlUpdate(sinkDDL)// flink catalog 中注册表

    //1.11 executeInsert / 1.10.0 使用 insertInto
    aggRes.executeInsert("jdbcOutputTable")

    env.execute("out to mysql")
  }

}
