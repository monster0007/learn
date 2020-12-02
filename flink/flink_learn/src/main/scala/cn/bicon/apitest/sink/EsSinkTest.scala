package cn.bicon.apitest.sink

import java.util

import cn.bicon.apitest.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Request, Requests}


object EsSinkTest{

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //source
    val dataStream = env.readTextFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\flink\\flink_learn\\src\\main\\resources\\sensor.txt")

    //transform
    val outStream = dataStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toInt, dataArray(2).trim.toDouble)
    })

    //sink Es
    val hosts = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("bd137",9200))

    val esSink = new ElasticsearchSink.Builder[SensorReading](hosts, new ElasticsearchSinkFunction[SensorReading] {
      /*def createIndexRequest(element: String): IndexRequest  = {
        val json = new util.HashMap[String,String]()
        json.put("data", element)
        Requests.indexRequest.index("my-index").`type`("my-type").source(json)
      }*/

      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val dataMap = new util.HashMap[String, String]()
        dataMap.put("sensor_id",t.id)
        dataMap.put("temperature",t.temperature.toString)
        dataMap.put("timestamp",t.timestamp.toString)

        val request = Requests.indexRequest().index("flinktest").source(dataMap)

        requestIndexer.add(request)
      }
    })

    //sink
    outStream.addSink(esSink.build())

    //execute
    env.execute("sink_Es")
  }
}
class EsSinkTest {

}
