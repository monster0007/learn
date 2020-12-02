package cn.bicon.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val parmas = ParameterTool.fromArgs(args)
    var host : String = parmas.get("host")
    var port : Int = parmas.getInt("port")

    //
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val textDataStream = env.socketTextStream(host, port).setMaxParallelism(4)

    val wordCountDataStream = textDataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print()

    env.execute("StreamWordCount")

  }

}
