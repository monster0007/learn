package cn.bicon.sparkStreaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-17 11:35
  *         P2 广告点击量实时统计
  *
  **/
object SparkStreaming_P2_Advertising{

  def main(args: Array[String]): Unit = {
    val streamingConfig = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(streamingConfig,Seconds(4))
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
      val day = uk.ts
      val area = uk.aera
      val city = uk.city
      val adid = uk.adid
      ((day,area,city,adid),1)
    }).reduceByKey(_ + _)


    clickDS.foreachRDD(rdd =>{
      rdd.foreachPartition(part =>{
        val conn = JdbcUtil.getConnection
        val psmt = conn.prepareStatement(
          """
            |insert into area_city_ad_count (dt,area,city,adid,count) values (?,?,?,?,?)
            |on duplicate key
            |update count = count + ?
          """.stripMargin)
        part.foreach{
          case ((day,area,city,adid),count) =>{

            psmt.setString(1,day)
            psmt.setString(2,area)
            psmt.setString(3,city)
            psmt.setString(4,adid)
            psmt.setInt(5,count)
            psmt.setInt(6,1)
            psmt.execute()

          }
        }
        psmt.close()
        conn.close()
      }

      )
    })







    ssc.start()
    ssc.awaitTermination()
  }

  case class userClick(ts: String,aera: String, city: String, adid: String)

}
