package cn.bicon.apitest.process

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  *案例1:温度传感器在 规定时间内连续上升
  * increaTime 毫秒值
  */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getCheckpointConfig.setCheckpointInterval(6000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    //重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,500))

    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //source
    var stream = env.socketTextStream("bd134",6666)
    //stream.print("input")
    // transform
    val dataStream = stream.map(data => {
      val dataArry = data.split(",")
      SensorReading(dataArry(0).trim, dataArry(1).trim.toLong, dataArry(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
    })

    //案例1:温度传感器在 规定时间内连续上
    val processedStream = dataStream
      .keyBy(_.id)
      .process(new TimeInCreAlter())
    //processedStream.print("processedStream")

    //案例2:温度变化差值 keyedProcessFunction
    val processStream2 = dataStream.keyBy(_.id)
      .process(new TempChangeAlter(10.0))
   // processStream2.print("TempChangeAlter")

    //案例2:温度变化差值 flatMap
    val flatMapStream = dataStream.keyBy(_.id)
      .flatMap(new TempChangeAlterFlatMap(10.0))
    //flatMapStream.print("flatMapStream")

    //案例2:温度变化差值 flatMapWithState
    val preocessStream3 = dataStream.keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double] {
      //如果没有状态的话,那么就把当前温度存入状态值
      case (input: SensorReading, None) => (List.empty, Some(input.temperature.toDouble))
      //如果有状态的话,那么就把当前温度状态值更新
      case (input: SensorReading, lastTemp: Some[Double]) =>
        val diff = (input.temperature - lastTemp.get).abs
        if (diff > 10.0) {
           (List((input.id, input.temperature.toDouble, lastTemp.get)), Some(input.temperature.toDouble))
        } else {
          (List.empty, Some(input.temperature.toDouble))
        }
    }
    preocessStream3.print("preocessStream3")


    env.execute("keyed process function")
  }
}


/**
  *案例1:温度传感器在 规定时间内连续上升
  * increaTime 毫秒值
  */
class TimeInCreAlter extends KeyedProcessFunction[String,SensorReading,String]{
  //保存上一个数据的温度 状态
  lazy val lastTemp : ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  //保存定时器
  lazy val currentTimer : ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer",classOf[Long]))

  var increaTime: Long = _


  //配置定时器
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //获取上个 温度值
    val perTemp = lastTemp.value()
    //更新lastTemp温度
    lastTemp.update(value.temperature.toDouble)
    //当前时间
    val currentTime = currentTimer.value()
    increaTime = 10000L
    //如果当前温度大于  并且没有注册定时器
    if( value.temperature > perTemp  && currentTime == 0 ){
      val timerTs = ctx.timerService().currentProcessingTime() + increaTime //连续时间
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }else if (perTemp > value.temperature || perTemp == 0.0){//清除时间
      ctx.timerService().deleteProcessingTimeTimer(currentTime)
      currentTimer.clear()
    }
  }

  //触发定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(ctx.getCurrentKey + "\twaring,已连续" + increaTime/ 1000 + "s上升")
    currentTimer.clear()
  }
}

/**
  * 案例2:温度变化差值
  */
class TempChangeAlter(threshold : Double) extends KeyedProcessFunction[String,SensorReading,String]{
lazy  private val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]( "lastTemp",classOf[Double]))

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //获取上次温度
    val prevTemp = lastTemp.value()
    val diff = (i.temperature - prevTemp).abs
    lastTemp.update(i.temperature.toDouble)
    if(diff > threshold) {
      collector.collect(context.getCurrentKey + "\t温度超过" + diff)
    }
  }
}

class TempChangeAlterFlatMap(threshold: Double) extends RichFlatMapFunction[SensorReading,String]{
  //存储温度状态值
  lazy private val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))

  override def flatMap(i: SensorReading, collector: Collector[String]): Unit = {
    //获取上次温度
    val prevTemp = lastTemp.value()
    val diff = (i.temperature - prevTemp).abs
    lastTemp.update(i.temperature.toDouble)
    if(diff > threshold) {
      collector.collect(i.id + "\t温度超过" + diff)
    }
  }
}

