package cn.bicon.tableapitest.udf

import cn.bicon.tableapitest.timeandwindons.SensorReadingw
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
//import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-17 16:52
  **/
object TableFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    //从调用开始给每个 env 创建一个stream追加时间特性
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置5秒产生一个 watermark
    env.getConfig.setAutoWatermarkInterval(5000)

    //source
    val stream = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_learn\\src\\main\\resources\\sensor.txt")
      .map(data => {
        val dataArry = data.split(",")
        SensorReadingw(dataArry(0).trim,dataArry(1).trim.toLong,dataArry(2).trim.toDouble)
      })
    val waterMarkStream = stream
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReadingw](Time.milliseconds(1000)) {
        override def extractTimestamp(t: SensorReadingw): Long = {
          t.timestamp * 1000
        }
      })

    //使用处理时间 系统时间
    val tablePro: Table = tableEnv.fromDataStream(waterMarkStream,'id,'temperature,'timestamp,'pt.proctime as 'ts)
    /* .toAppendStream[Row]
     .print("process time")*/
    // 使用事件时间
    val tableEvent: Table = tableEnv.fromDataStream(waterMarkStream,'id,'temperature,'timestamp.rowtime as 'ts)
    /*.toAppendStream[Row]
    .print("event time")*/

    //table api 调用自定义函数
    val split = new Split("_")
    tableEvent
        .joinLateral(split('id) as ('word,'length))
      .select('id,'ts,'word,'length)
      .toAppendStream[Row]
      //.print("table api use tableUDF")

    //table sql use UDF
    //注册表
    tableEnv.createTemporaryView("sensor",tableEvent)
    //注册UDF
    tableEnv.registerFunction("split",split)
    tableEnv.sqlQuery(
      """
        |select id,ts,word,length
        |from
        |sensor,lateral table(split(id)) as splitid(word,length)
      """.stripMargin)
      .toAppendStream[Row]
      .print("sql use udf")
    env.execute("job of udf")
  }

  class Split(separator: String ) extends TableFunction[(String,Int)]{
    def eval(str: String): Unit ={
      str.split(separator).foreach(
        word => collect((word,word.length))
      )
    }

  }
}
