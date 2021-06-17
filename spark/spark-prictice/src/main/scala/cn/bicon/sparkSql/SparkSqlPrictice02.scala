package cn.bicon.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-16 15:58
  **/
object SparkSqlPrictice02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Product Category Top3")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import sparkSession.implicits._

     sparkSession.sql(
      """
        |SELECT
        |c.area,
        |b.product_name,
        |c.city_name
        |FROM
        |user_visit_action a
        |LEFT JOIN product_info b ON a.click_product_id = b.product_id
        |LEFT JOIN city_info c ON a.city_id = c.city_id
        |WHERE  a.click_product_id !=-1
      """.stripMargin).createOrReplaceTempView("t1")

    val cityRemark2  =  new CityRemarkUDAF2()
    sparkSession.udf.register("cityRemark2",cityRemark2)


   sparkSession.sql(
      """
        |SELECT
        |area,
        |product_name,
        |count(*) clickCount,
        |cityRemark2(city_name) city_remark
        |FROM t1
        |GROUP BY area,product_name
      """.stripMargin).createOrReplaceTempView("t2")


    sparkSession.sql(
      """
        |SELECT
        |t2.area,
        |t2.product_name,
        |t2.clickCount,
        |t2.city_remark,
        |rank() over(partition by t2.area order by t2.clickCount desc) rank
        |FROM t2
      """.stripMargin).createOrReplaceTempView("t3")

    sparkSession.sql(
      """
        |
        |SELECT
        |t3.area,
        |t3.product_name,
        |t3.clickCount,
        |t3.rank,
        |t3.city_remark
        |FROM t3
        |where rank <=3
      """.stripMargin).show(false)

    sparkSession.close()

  }


  case class BUffer(var total: Long,var city: mutable.Map[String,Long])

  /**
    * 1.集成Aggregator
    * IN: 输入cityName
    * BUF:[总点击数,Map((city,cnt),(city2,cnt))]
    * OUT:输出省市备注
    *
    */
  class CityRemarkUDAF extends Aggregator[String,BUffer,String]{
    //判断 是否为初始值
    override def zero: BUffer = {
      BUffer(0,mutable.Map[String,Long]())
    }

    //更新分区内 缓冲区
    override def reduce(buf: BUffer, city: String): BUffer = {
      buf.total += 1
      val newCount = buf.city.getOrElse(city,0L) + 1
      buf.city.update(city,newCount)
      buf
    }

    //合并map
    override def merge(b1: BUffer, b2: BUffer): BUffer = {
      b1.city.foreach {
        case (city, cnt) => {
          val newCount = b2.city.getOrElse(city, 0L) + cnt
          b1.city.update(city,newCount)
        }
      }
      b1
    }

    //返回值
    override def finish(buf: BUffer): String = {
      val remarkList = ListBuffer[String]()
      //总数
      val totalcnt = buf.total
      val cityMap = buf.city

      //排序
      val cityCntList = cityMap.toList.sortWith(
        (left, right) => {
          left._2 > right._2 //降序
        }
      )
        .take(2)//排序完成后取前两个

      val haseMore = cityMap.size > 2
      var r2 = 0L

      cityCntList.foreach{
        case (city,cnt) =>{
          val rate  = cnt * 100 / totalcnt
          r2 += rate
          remarkList.append(s"${city} $rate %")
        }
      }


      if(haseMore){
        cityCntList.foreach{
          case (city,cnt) =>{
            val rate  = cnt * 100 / totalcnt
            remarkList.append(s"其它 ${1-r2} %")
          }
        }
      }
      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[BUffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }



  class CityRemarkUDAF2 extends UserDefinedAggregateFunction{
    //输入数据类型
    override def inputSchema: StructType = {
      StructType(Array(StructField("city",StringType)))
    }

    //缓存区数据类型Map[city,cnt] ->   Map[String,Long]
    override def bufferSchema: StructType = {
      StructType(Array(StructField("total",LongType),StructField("cityMap", MapType.apply(StringType,LongType))))
    }

    //返回值数据类型
    override def dataType: DataType = StringType

    //稳定性: 对于相同的输入是否返回相同的输出
    override def deterministic: Boolean = true

    //缓存冲区初始值
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      //println(buffer)
      buffer(0) = 0L
      buffer(1) = mutable.Map[String,Long]()
    }

    //分区内数据聚合
    override def update(buffer: MutableAggregationBuffer, city: Row): Unit = {
      val total = buffer.getLong(0) + 1L
      var cityMap = buffer.getMap[String,Long](1)
      val cityName = city.getString(0)
      val cnt = cityMap.getOrElse(cityName,0L) + 1
      buffer.update(1,cityMap.updated(cityName,cnt))
      buffer.update(0,total)
    }

    //分区间聚合
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      var t1 = buffer1.getLong(0)
      var t2 = buffer2.getLong(0)
      var map1 = buffer1.getMap[String,Long](1)
      var map2 = buffer2.getMap[String,Long](1)
      //println("map1:" + map1)
      //println("map2:" + map2)
     /* map1.foldLeft(map2) {
          case (map, kv) => {
            var newCnt = map.getOrElse(kv._1, 0L) + 1
            map.updated(kv._1, newCnt)
            map
          }
      }*/
      map2.foreach{
        case (city,cnt) => {
          val t = map1.getOrElse(city,0L) + 1
          map2.updated(city,t)
          map2
        }

      }
      buffer1.update(0,t1 + t2)
      buffer1.update(1,map2)
    }
    //返回值处理
    override def evaluate(buffer: Row): String = {
      val resList = ListBuffer[String]()

      val total = buffer.getLong(0)
      var sumTal = 0L
      val map = buffer.getMap[String,Long](1)

      //根据cnt排序取出前两个城市
      val cityList = map.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }.take(2)

      var hasMore = map.size > 2
      //遍历请求前两个城市的占比,并求取前两个城市的占比和
      cityList.foreach{
        case(city, cnt) =>{
           var rate = cnt * 100 / total
          sumTal += rate
          resList.append(s"${city} $rate %")
        }
      }

      //如果分区内聚合结果 大于两个城市,计算为其它
      if(hasMore){
         resList.append(s"其它 ${100-sumTal} %")
      }
      resList.mkString(",")
    }
  }
}
