package cn.bicon.apitest.sink

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config._
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //source
    val dataStream = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_learn\\src\\main\\resources\\sensor.txt")

    //transform
    val outStream = dataStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toInt, dataArray(2).trim.toDouble)
    })
    val conf:FlinkJedisConfigBase = new FlinkJedisPoolConfig.Builder()
      .setHost("bd137")
      .setPort(27000)
      .setPassword("bicon.redis.test@123456")
      .setDatabase(2)
      .build()

    //sink
    outStream.addSink(new RedisSink(conf,new MyRedisMapper))

    // execute
    env.execute("sink_redis")
  }
}

class MyRedisMapper() extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    val command = RedisCommand.HSET
    new RedisCommandDescription(command,"testRedis")
  }

  override def getKeyFromData(t: SensorReading): String = {
    t.id
  }

  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }
}