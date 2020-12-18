package cn.bicon.tableapitest.udf

import cn.bicon.tableapitest.timeandwindons.SensorReadingw
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-18 09:30
  **/
object AggregateFunctionTest {
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

    // table api use aggregate function
    val avg = new AvgTem()
    tableEvent
      .groupBy('id)
      .aggregate(avg('temperature) as 'avgTemp)
      .select('id,'avgTemp)
      .toRetractStream[Row]
      .print("table api")

    //table sql use aggregate function
    tableEnv.createTemporaryView("sensor",tableEvent)
    tableEnv.registerFunction("temperatureAvg",avg)
    tableEnv.sqlQuery(
      """
        |select
        |id,temperatureAvg(temperature) as temp
        |from sensor
        |group by id
      """.stripMargin)
        .toRetractStream[Row]
        .print("table sql")

    env.execute("aggregate function")

  }

  //自定义状态类
  class  AvgTemAcc{
    var sum: Double = 0.0
    var count: Int = 0
  }

  case class AvgTem()  extends AggregateFunction[Double,AvgTemAcc]{
    override def getValue(accumulator: AvgTemAcc): Double = accumulator.sum/accumulator.count

    override def createAccumulator(): AvgTemAcc = new AvgTemAcc()

    def accumulate(acc: AvgTemAcc,temp: Double): Unit ={
      acc.sum += temp
      acc.count += 1
    }
  }

}
