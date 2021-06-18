package cn.bicon.test.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-17 11:35
  **/
object SparkStreaming_05State {

  def main(args: Array[String]): Unit = {
    val streamingConfig = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(streamingConfig,Seconds(3))
    ssc.checkpoint("checkpoint")

    val ds = ssc.socketTextStream("localhost",999)
    val ds2 = ds.flatMap(action => {
      action.split(" ")
    }).map(action => {
      (action, 1)
    })
    //状态
    //第一个参数表示相同key的value数据
    //第二个参数表示缓存区相同key的value数据
    val resDS = ds2.updateStateByKey(
      (seq: Seq[Int], buf: Option[Int]) => {
        val newCount = buf.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )
    resDS.print()



    ssc.start()
    ssc.awaitTermination()

  }

}
