package cn.bicon.test.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-17 11:35
  **/
object
SparkStreaming_02Queue {

  def main(args: Array[String]): Unit = {
    val streamingConfig = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(streamingConfig,Seconds(4))
    //创建rdd 队列
  val rddQueue =new mutable.Queue[RDD[Int]]()

    val ds: InputDStream[Int] = ssc.queueStream(rddQueue,false)
    ds.map((_,1)).reduceByKey(_ + _)
      .print()

    ssc.start()

    for(i <- 0 until 5){
     rddQueue += ssc.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(2000)

    }

    ssc.awaitTermination()

  }

}
