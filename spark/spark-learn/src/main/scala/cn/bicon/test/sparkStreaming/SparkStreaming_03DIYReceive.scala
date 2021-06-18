package cn.bicon.test.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-17 11:35
  **/
object SparkStreaming_03DIYReceive{

  def main(args: Array[String]): Unit = {
    val streamingConfig = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(streamingConfig,Seconds(4))
    val receive = new MyReceive
    val input: ReceiverInputDStream[String] = ssc.receiverStream(receive)


    input.print()


    ssc.start()
    ssc.awaitTermination()

    receive.onStop()


  }

  class MyReceive extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flag = true

    override def onStart(): Unit = {
      val i = 0
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flag) {
            val message = "采集数据" + i+1
            store(message)
          }
        }
      }).start()


    }

    override def onStop(): Unit = {
      flag=false
    }
  }

}
