package cn.bicon.sparkStreaming

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.parquet.schema.Types.ListBuilder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-17 11:35
  **/
object SparkStreaming_P1_MockData{

  def main(args: Array[String]): Unit = {
    val streamingConfig = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(streamingConfig,Seconds(4))
    //1.创建kafka 配置参数
    val kafkaPara = new java.util.HashMap[String, Object](
    )
    kafkaPara.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"bd138:19092")
    kafkaPara.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    kafkaPara.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    //2 创建producer
    val producer = new KafkaProducer[String, String](kafkaPara)
    while (true){
      val list = mockData
      list.foreach(line => {
        println(line)
        var record = new ProducerRecord[String,String]("atguigu",line)
        producer.send(record)
      })
      Thread.sleep(2000)
    }

    ssc.start()
    ssc.awaitTermination()
  }


  def mockData() : ListBuffer[String] ={
    val listBuf = ListBuffer[String]()

    val area = List("华北","华东","华西","东南")
    val city = List("上海","苏州","杭州","北京")
    val time = System.currentTimeMillis()

    for(i <- 0 to  50) {
      val userid = new Random().nextInt(6) + 1
      val advid = new Random().nextInt(6) + 1
      listBuf.append(s"$time ${area(new Random().nextInt(3))} ${city(new Random().nextInt(3))} $userid $advid")
    }
    listBuf
  }


}
