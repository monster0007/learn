package cn.bicon.tableapitest.sourceandsink
import cn.bicon.apitest.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._// flink 1.11.1
//import org.apache.flink.table.api.scala._// flink 1.10.0
/**
 * @Author: shiyu
 * @Date: 2020/12/14 0014 14:08
 */
object Example {
  def main(args: Array[String]): Unit = {
    //0 执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val stremFromFile = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_learn\\src\\main\\resources\\sensor.txt")

    val dataStream = stremFromFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    //创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //转化为tableStream
    val dataTable = tableEnv.fromDataStream(dataStream)


    //1.1table api
    val tableAPIStream = dataTable.select("id,temperature")
      .filter("id = 'sensor_1'")
    //1.2把tableStream 转化为 stream 查看输出结果
    val resultStream = tableAPIStream.toAppendStream[(String,BigDecimal)]
    resultStream.print("resultStream")

    //2.1
    tableEnv.registerTable("test", dataTable)
    val tableSqlStream = tableEnv.sqlQuery("select id, temperature from test where id = 'sensor_1'")
    val resultSqlStream = tableSqlStream.toAppendStream[(String, BigDecimal)]
    resultSqlStream.print("sql")

    env.execute("table api and table sql")
  }
}
