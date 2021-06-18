package cn.bicon.sparkStreaming

import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-17 11:35
  *         P2 广告点击量实时统计
  *
  **/
object SparkStreaming_P3_OneHourAdClick{

  def main(args: Array[String]): Unit = {
    val streamingConfig = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(streamingConfig,Seconds(2))
    //创建kafka 配置参数
    val kafkaPara =  Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "bd138:19092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu3",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val input: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set("atguigu"), kafkaPara))

    val clickDS = input.map(line => {
      val data = line.value()
      val datas = data.split(" ")
      userClick(datas(0), datas(1), datas(2), datas(4))
    }).map(uk =>{
      val day = uk.ts.toLong
      val sf = new SimpleDateFormat("mm:ss")
      val timestr = sf.format(new java.util.Date(day))
      (timestr,1)
    })
      .reduceByKeyAndWindow((x: Int,y: Int) =>{x + y},Seconds(60),Seconds(2))



    clickDS.print()






    ssc.start()
    ssc.awaitTermination()
  }

  case class userClick(ts: String,aera: String, city: String, adid: String)

}
