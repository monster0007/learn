package cn.bicon.test.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-17 11:35
  **/
object
SparkStreaming_01WordCount {

  def main(args: Array[String]): Unit = {
    val streamingConfig = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(streamingConfig,Seconds(3))

    val ds = ssc.socketTextStream("localhost",999)
       ds.flatMap(action => {
      action.split(" ")
    }).map(action => {
      (action, 1)
    }).reduceByKey(_ + _)
      .print()




    ssc.start()
    ssc.awaitTermination()

  }

}
