package cn.bicon.test.sparkCore.rdd.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-24 13:30
  **/
object RDDPart {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(("nab","nabxxx"),("cba","nabxxx"),("wba","nabxxx")))
    val partRdd = rdd.partitionBy(new MyPartioner(3))
    partRdd.saveAsTextFile("output")


  }

  class MyPartioner(partnum: Int) extends Partitioner  {
    override def numPartitions: Int = partnum

    override def getPartition(key: Any): Int = {
      key match {
        case "nab" => 0
        case "cba" => 1
        case  _ => 2
      }
    }
  }

}
