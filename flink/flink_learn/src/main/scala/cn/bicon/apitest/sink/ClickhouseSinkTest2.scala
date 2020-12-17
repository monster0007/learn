/*
package cn.bicon.apitest.sink

import java.sql.PreparedStatement

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}



object ClickhouseSinkTest2 {

  def main(args: Array[String]): Unit = {

    val SourceCsvPath = "D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\flinkLearn\\src\\main\\resources\\sensor.txt"
    val CkJdbcUrl = "jdbc:clickhouse://bd134:18123"
    val CkUsername = "default"
    val CkPassword = ""
    val BatchSize = 5 // 设置您的batch size


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)
    //source
    val dataStream = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_learn\\src\\main\\resources\\sensor.txt")

    //transform
    val outStream = dataStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toInt, dataArray(2).trim.toDouble)
    })

    outStream.print()

    //sink
    outStream.addSink(
      JdbcSink.sink(
        "INSERT INTO temperature (sensor,temperature) values (?,?)",
        new JdbcStatementBuilder[SensorReading]() {
          override def accept(ps: PreparedStatement, t: SensorReading): Unit = {
            ps.setString(1, t.id)
            ps.setBigDecimal(2, t.temperature.bigDecimal)
          }
        },
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl(CkJdbcUrl)
          .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
          .withUsername(CkUsername)
          .withPassword("")
          .build())
    )

    //execute
    val result = env.execute()

  }
}
*/
