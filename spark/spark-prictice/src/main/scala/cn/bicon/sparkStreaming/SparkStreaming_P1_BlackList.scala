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
  *         P1 广告黑名单
  *
  **/
object SparkStreaming_P1_BlackList{

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
      userClick(datas(0).toLong, datas(1), datas(2), datas(3), datas(4))
    })


    val ds = clickDS.transform(
      rdd => {
        //周期性执行:(周期性查询mysql black_list 名单)
        val blackList = ListBuffer[Int]()
        val conn = JdbcUtil.init().getConnection
        val ps = conn.prepareStatement("select userid from black_list")
        val rs = ps.executeQuery()
        while (rs.next()) {
          val userid = rs.getInt(1)
          blackList.append(userid)
        }
        rs.close()
        ps.close()
        conn.close()

        val filterRDD = rdd.filter(fliter => {
          !blackList.contains(fliter.userid) //不包含黑名单的数据
        })

        filterRDD.map(data => {
          val format = new SimpleDateFormat("yyyy-MM-dd")
          val day = format.format(new Date(data.ts))
          ((day, data.userid, data.adid), 1)
        }).reduceByKey(_ + _)
      }
    )


    //TODO 1超出阈值(30) 加入黑名单
    //TODO 2如果用户点击没有超过阈值(30) 则更新黑名单
    //TODO 3更新之后的数据继续判读是否超过阈值 ,超过则加入黑名单
    ds.foreachRDD(rdd =>{
      // rdd. foreach 方法每条数据都会创建一个连接
      //可以使用foreachPartition 代替foreach ,按照每个分区遍历数据提升效率
      rdd.foreach{
        case((day, userid, adid), count)=>{
          if(count >= 30){    //TODO 1超出阈值(30) 加入黑名单
            val conn = JdbcUtil.getConnection
            val pstm = conn.prepareStatement(
              """
                |insert into black_list(userid) values (?)
                |ON DUPLICATE KEY
                |UPDATE userid=?
              """.stripMargin)
            pstm.setString(1,userid)
            pstm.setString(2,userid)
            pstm.executeUpdate()
            pstm.close()
            conn.close()
          }else{//TODO 2如果用户点击没有超过阈值(30) 则更新黑名单
          //查询统计表数据
          val conn = JdbcUtil.getConnection
            val pstm = conn.prepareStatement(
              """
                |select
                |*
                |from
                |user_ad_count
                |where dt=? and userid=? and adid=?
              """.stripMargin)
            pstm.setString(1,day)
            pstm.setString(2,userid)
            pstm.setString(3,adid)
            val rs = pstm.executeQuery()

            //如果存在则更新
            if(rs.next()){
              val conn = JdbcUtil.getConnection
              val pstm = conn.prepareStatement(
                """
                  |update
                  |user_ad_count
                  |set count = count + ?
                  |where  dt=? and userid=? and adid=?
                """.stripMargin)
              pstm.setLong(1,1)
              pstm.setString(2,day)
              pstm.setString(3,userid)
              pstm.setString(4,adid)
              pstm.executeUpdate()
              pstm.close()
              //TODO 3更新之后的数据继续判读是否超过阈值 ,超过则加入黑名单
              val pstm4 = conn.prepareStatement(
                """
                  |select
                  |*
                  |from
                  |user_ad_count
                  |where dt=? and userid=? and adid=? and count >=30
                """.stripMargin)
              pstm4.setString(1,day)
              pstm4.setString(2,userid)
              pstm4.setString(3,adid)
              val rs2 = pstm4.executeQuery()
              if(rs2.next()){//如果有则加入黑名单
                println("rs2如果有则加入黑名单")
                val pstm3 = conn.prepareStatement(
                  """
                    |insert into black_list(userid) values (?)
                    |ON DUPLICATE KEY
                    |UPDATE userid=?
                  """.stripMargin)
                pstm3.setString(1,userid)
                pstm3.setString(2,userid)
                pstm3.executeUpdate()
                pstm3.close()
              }
              rs2.close()
              pstm4.close()
            }else{ //不存在则插入
              val pstm1 = conn.prepareStatement(
                """
                  |insert into user_ad_count (dt,userid,adid,count) values(?,?,?,?)
                  |""".stripMargin)
              pstm1.setString(1,day)
              pstm1.setString(2,userid)
              pstm1.setString(3,adid)
              pstm1.setLong(4,count)
              pstm1.executeUpdate()
              pstm1.close()
            }

            rs.close()
            pstm.close()
            conn.close()

          }

        }
      }
    })


    ssc.start()
    ssc.awaitTermination()
  }

  case class userClick(ts: Long,aera: String, city: String, userid: String, adid: String)

}
