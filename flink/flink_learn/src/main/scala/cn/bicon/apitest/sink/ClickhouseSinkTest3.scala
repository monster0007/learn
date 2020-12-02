package cn.bicon.apitest.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object ClickhouseSinkTest3 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //source
    val dataStream = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\flinkLearn\\src\\main\\resources\\sensor.txt")

    //transform
    val outStream = dataStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toInt, dataArray(2).trim.toDouble)
    })

    //jdbc sink
    outStream.addSink(new ClickHouse())

    //execute
    env.execute("jdbc_test")
  }
}



class ClickHouse extends RichSinkFunction[SensorReading]{
  //预编译器
  var clickhourseUrl = "jdbc:clickhouse://10.1.24.134:18123"
  var connection: Connection = null
  var insert: PreparedStatement = null
  var update: PreparedStatement = null

  //建立连接
  override def open(parameters: Configuration): Unit = {
    connection = DriverManager.getConnection(clickhourseUrl, "default", "")
  }

  //释放资源
  override def close(): Unit = {
    update.close()
    insert.close()
    connection.close()
  }

  //处理数据
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    update = connection.prepareStatement(s"ALTER TABLE temperature UPDATE temperature=? WHERE sensor=?")

    update.setBigDecimal(1,value.temperature.bigDecimal)
    update.setString(2,value.id)
    update.executeUpdate()

    insert = connection.prepareStatement("INSERT INTO temperature (sensor,temperature) values (?,?)")
    if(update.getUpdateCount == -1){
      insert.setString(1,value.id)
      insert.setBigDecimal(2,value.temperature.bigDecimal)
      insert.executeUpdate()
    }
  }
}