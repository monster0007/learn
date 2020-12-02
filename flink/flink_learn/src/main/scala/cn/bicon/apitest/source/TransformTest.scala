package cn.bicon.apitest.source

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object TransformTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val stremFromFile = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_learn\\src\\main\\resources\\sensor.txt")

    val dataStram = stremFromFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .keyBy(0)
      .reduce((x,y) =>{
        SensorReading(x.id,x.timestamp + 1,y.temperature + 10)
      })
      //.sum(2)

    //dataStram.print("keyStream")

    //2.多流转化操作-分流
    val dataStram2 = stremFromFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
    val splitStream = dataStram2.split(data => {
      if (data.temperature > 30) Seq("High") else Seq("Low")
    })
    val highStream = splitStream.select("High")
    val lowStream = splitStream.select("Low")
    val all = splitStream.select("High","Low")
    //highStream.print("high")
    //lowStream.print("Low")
    //all.print("all")

    //3.connect 和Comap  合流
    val warning = highStream.map(data => (data.id,data.temperature))
    val connectedStream = warning.connect(lowStream)

    val coMapDataStream = connectedStream.map(
      waringData => (waringData._1,waringData._2,"warning"),
      lowData => (lowData.id,"healthy")
    )

    //coMapDataStream.print()

    //4.函数类
    dataStram.filter(new MyFilter()).print("filterFunction")


    env.execute("transform")


  }

}

class MyFilter extends  FilterFunction[SensorReading](){
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}
