package cn.bicon.wc

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}


object WordCount{

  def main(args: Array[String]): Unit = {
    //创建批处理执行环境
    val environment = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\flinkLearn\\src\\main\\resources\\hello.txt"
    val inputDateSet = environment.readTextFile(inputPath)

    val wordCountDataSet = inputDateSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()


  }

}



class WordCount {

}
