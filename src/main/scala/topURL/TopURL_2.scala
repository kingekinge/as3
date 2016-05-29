package topURL

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhang on 2016/4/12.
  */
object TopURL_2 extends  App {

  private val sparkConf = new SparkConf().setAppName("分组排序").setMaster("local")
  private val sparkContext: SparkContext = new SparkContext(sparkConf)

  val rdd=sparkContext.textFile("C:/spark_data/itcast.log").map(k => {
    val sp = k.split("\t")
    val url = sp(1)
    (url, 1)
  }).reduceByKey(_ + _).map(x => {
    val host: String = new URL(x._1).getHost
    (host, x._1, x._2)
  }).groupBy(_._1).map(_._2.toList.sortBy(_._2).reverse.take(2)).collect().toBuffer

  println(rdd)



}
