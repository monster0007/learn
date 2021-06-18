package cn.bicon.test.sparkStreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-17 11:35
  **/
object SparkStreaming_04Kafka{

  def main(args: Array[String]): Unit = {
    val streamingConfig = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(streamingConfig,Seconds(4))
    //创建kafka 配置参数
    val kafkaPara =  Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "bd138:19092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val input: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set("atguigu"), kafkaPara))

    input.map(_.value()).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
