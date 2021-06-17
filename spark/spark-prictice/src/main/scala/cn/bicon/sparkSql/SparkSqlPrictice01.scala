package cn.bicon.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @program: learn
  * @author: shiyu
  * @create: 2021-06-16 15:58
  **/
object SparkSqlPrictice01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Product Category Top3")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    sparkSession.sql(
      """
        |CREATE TABLE  IF NOT EXISTS `user_visit_action`(
        | `date` string,
        | `user_id` bigint,
        | `session_id` string,
        | `page_id` bigint,
        | `action_time` string,
        | `search_keyword` string,
        | `click_category_id` bigint,
        | `click_product_id` bigint,
        | `order_category_ids` string,
        | `order_product_ids` string,
        | `pay_category_ids` string,
        | `pay_product_ids` string,
        | `city_id` bigint)
        |row format delimited fields terminated by '\t'
      """.stripMargin)

    /*val rdd = sparkSession.sparkContext.textFile("D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\spark\\spark-prictice\\input\\user_visit_action.txt")
    rdd.foreach(println)*/

    //sparkSession.sql("load data local inpath 'D:\\softwaresetup\\IT\\workspace\\2020ideaworkspace\\learn\\spark\\spark-prictice\\input\\user_visit_action.txt' into table user_visit_action")


    sparkSession.sql(
      """
        |
        |SELECT
        |t3.area,
        |t3.product_name,
        |t3.clickCount,
        |t3.rank
        |FROM(
        |SELECT
        |t2.area,
        |t2.product_name,
        |t2.clickCount,
        |rank() over(partition by t2.area order by t2.clickCount desc) rank
        |FROM
        |(
        |SELECT
        |t1.area,
        |t1.product_name,
        |count(*) as clickCount
        |FROM (
        |SELECT
        |a.*,
        |b.product_name,
        |c.city_name,
        |c.area
        |FROM
        |user_visit_action a
        |LEFT JOIN product_info b ON a.click_product_id = b.product_id
        |LEFT JOIN city_info c ON a.city_id = c.city_id
        |WHERE  a.click_product_id !=-1 ) t1
        |GROUP BY t1.area,t1.product_name
        |ORDER BY clickCount DESC ) t2
        |) t3
        |where rank <=3
      """.stripMargin).show()




    sparkSession.close()

  }

}
