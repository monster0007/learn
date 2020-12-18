package cn.bicon.tableapitest.udf

import cn.bicon.tableapitest.timeandwindons.SensorReadingw
import cn.bicon.tableapitest.udf.TableFunctionTest.Split
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-18 10:35
  * 案例: 表聚合函数 求 top2
  **/
object TableAggFunctionTest {
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
    val top2 = new Top2Acc()
    tableEvent
      .groupBy('id)
      .flatAggregate(top2('temperature) as ('temp,'rank))
      .select('id,'temp,'rank)
      .toRetractStream[Row]
      //.print("table api")

    // table sql use TableAggFunction
    //注册表
    tableEnv.createTemporaryView("sensor",tableEvent)
    //注册函数
    tableEnv.registerFunction("top2",top2)
    /*
  tableEnv.sqlQuery(
    """
      |select
      |'id,top2('temperature) as temp
      |from sensor
      |group by id
    """.stripMargin)
      .toRetractStream[Row]
      .print("table sql")*/
    env.execute("table api and sql")
  }
  //定义状态类
  class Top2TempAcc(){
    var hightest: Double = Double.MinValue
    var secondeHightest: Double = Double.MinValue
  }

  class Top2Acc() extends TableAggregateFunction[(Double,Int),Top2TempAcc]{
    //初始化状态值
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc()
    //计算状态值,每来一条数据进行的操作
    def accumulate(acc: Top2TempAcc,temp: Double): Unit ={
      //当前温度和第一个最高的温度比
      if(temp > acc.hightest){
        acc.secondeHightest = acc.hightest
        acc.hightest = temp
      }else if(temp > acc.secondeHightest){// 当前温度和第二个温度达则替换第二个温度
        acc.secondeHightest = temp
      }
    }

    //输出结果到临时表中
    def emitValue(acc: Top2TempAcc,out: Collector[(Double,Int)]): Unit ={
      out.collect((acc.hightest,1))
      out.collect((acc.secondeHightest,2))
    }
  }


}
