package cn.itcast.spark

import java.net.URL

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by ZX on 2016/4/12.
  */
object UrlTopN_4 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("c://itcast.log").distinct()

    val partArr = data.map(x => {
      val lines = x.split("\t")
      val host = new URL(lines(1)).getHost
      host
    }).distinct().collect()


    val partitioner = new HostPartitioner(partArr)

    data.map(x => {
      val lines = x.split("\t")
      (lines(1),1)
    }).partitionBy(partitioner).reduceByKey(_+_).mapPartitions(_.toList.sortBy(_._2).reverse.iterator).saveAsTextFile("c:/out00000")
    sc.stop()


  }
}

class HostPartitioner(arr: Array[String]) extends Partitioner {

  val partMap = new mutable.HashMap[String, Int]()
  var counter = 0

  for(host <- arr){
    partMap += (host -> counter)
    counter += 1
  }

  override def numPartitions: Int = arr.length

  override def getPartition(key: Any): Int = {
    val host: String = new URL(key.toString).getHost
    partMap.getOrElse(host, 0)
  }
}
