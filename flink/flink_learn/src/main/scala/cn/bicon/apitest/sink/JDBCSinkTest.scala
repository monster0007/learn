package cn.bicon.apitest.sink

import java.sql.{Connection, DriverManager, PreparedStatement, Statement}

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object JDBCSinkTest{
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
    outStream.addSink(new MySQLSink)

    //execute
    env.execute("jdbc_test")

  }
}


class JDBCSinkTest {

}

class MySQLSink extends RichSinkFunction[SensorReading]{
  //预编译器
  val mysqUrl  = "jdbc:mysql://bd208:3306/test"
  var orclUrl = "jdbc:oracle:thin:@bd134:1521:orcl"
  var clickhourseUrl = "jdbc:clickhouse://10.1.24.134:18123"
  var connection: Connection = null
  var insert: PreparedStatement = null
  var update: PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //connection = DriverManager.getConnection(mysqUrl, "root", "bicon@123")
    connection = DriverManager.getConnection(orclUrl, "debezium", "debezium")
    //connection = DriverManager.getConnection(clickhourseUrl, "default", "")
  }

  override def close(): Unit = {
    update.close()
    insert.close()
    connection.close()
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    println(connection.getMetaData.getDriverName)
    update = connection.prepareStatement("UPDATE temperature set temperature =? WHERE sensor = ?")
    //update = connection.prepareStatement("ALTER TABLE default.temperature UPDATE temperature=? WHERE city=?")

    insert = connection.prepareStatement("INSERT INTO temperature (sensor,temperature) values (?,?)")

    update.setBigDecimal(1,value.temperature.bigDecimal)
    update.setString(2,value.id)
    update.executeUpdate()

    if(update.getUpdateCount == 0){
      insert.setString(1,value.id)
      insert.setBigDecimal(2,value.temperature.bigDecimal)
      insert.executeUpdate()
    }
  }
}