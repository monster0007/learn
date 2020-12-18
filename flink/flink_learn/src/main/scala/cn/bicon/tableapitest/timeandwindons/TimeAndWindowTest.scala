package cn.bicon.tableapitest.timeandwindons

import java.sql.Timestamp

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-17 11:01
  * 时间语义的使用
  **/
case  class SensorReadingw(id : String, timestamp : Long, temperature : Double)

object TimeAndWindowTest {
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
    val tablePro: Table = tableEnv.fromDataStream(waterMarkStream,'id,'temperature,'timestamp,'pt.proctime() as 'ts)
       /* .toAppendStream[Row]
        .print("process time")*/
    // 使用事件时间
    val tableEvent: Table = tableEnv.fromDataStream(waterMarkStream,'id,'temperature,'timestamp.rowtime() as 'ts)
      /*.toAppendStream[Row]
      .print("event time")*/

    //1.1table api group window 聚合操作
    tableEvent
        .window(Tumble over 10.seconds() on 'ts as 'tw)//按照时间语义进行开窗 滚动窗口
        .groupBy('tw,'id)
        .select('id,'id.count(),'tw.start(),'tw.end()).toAppendStream[Row]
      //.print("agg")

    tableEvent
      .window(Slide over 10.seconds() every 5.seconds()  on 'ts as 'tw)//按照时间语义进行开窗 滑动窗口
      .groupBy('tw,'id)
      .select('id,'id.count(),'tw.start(),'tw.end()).toAppendStream[Row]
    //.print("agg")


    //1.2 table api over window 聚合操作
    tableEvent
        .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
        .select('id,'ts,'id.count over 'ow,'temperature.avg over 'ow)
        .toAppendStream[Row]
        //.print("over window")

    //2.1 sql group window 聚合操作
    //注册表
    tableEnv.createTemporaryView("sensor",tableEvent)

    /**
      * -- 滑动窗口
      * hop(timestamp,interval ,interval)  滑动窗口 ,
      * hop_start(timestamp,interval ,interval) 窗口开始时间
      * hop_end(timestamp,interval ,interval) 窗口结束时间
      * -- 滚动窗口
      * tumble(timestamp,interval) -- 滚动窗口
      *tumble_start(timestamp,interval)
      *tumble_end(timestamp,interval)
      */
    //滚动
    tableEnv.sqlQuery(
      """
        |select id,count(id),
        |tumble_start(ts, interval '10' second) , -- window start
        |tumble_end(ts, interval '10' second)  -- window start
        |from sensor
        |group by id,tumble(ts, interval '10' second)  -- 滚动窗口
      """.stripMargin)
      .toAppendStream[Row]
      //.print("sql tumble group window")

    //  滑动窗口
    tableEnv.sqlQuery(
      """
        |select id,count(id),
        |hop_start(ts,interval '5' second, interval '10' second) , -- window start
        |hop_end(ts,interval '5' second, interval '10' second)  -- window start
        |from sensor
        |group by id,hop(ts,interval '5' second, interval '10' second)  -- 滑动窗口
      """.stripMargin)
        .toAppendStream[Row]
        //.print("sql slide group window")

    //2.2 sql over window 聚合操作
    tableEnv.sqlQuery(
      """
        |select id,count(id) over w,avg(temperature) over w
        |from sensor
        |window w as (
        |partition by id
        |order by ts
        |rows between 2 preceding and current row
        |)
      """.stripMargin)
      .toAppendStream[Row]
        .print("sql over window tumble")


    env.execute("water mark")
  }

}
