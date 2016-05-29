package topURL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhang on 2016/4/12.
  */
object TopURL_1 extends App{


  private val sparkConf: SparkConf = new SparkConf()
  sparkConf.setAppName("分组排序").setMaster("local")
  private val sparkContext: SparkContext = new SparkContext(sparkConf)

  var domains = Array("http://java.itcast.cn", "http://php.itcast.cn", "http://net.itcast.cn")

  val rdd=sparkContext.textFile("C:/spark_data/itcast.log").map(k=>{
    val split: Array[String] = k.split("\t")
    val url=split(1)
    (url,1)
  }).reduceByKey(_+_)

  for(domain <-domains){
    //过滤出以主机名开头的url
      val tuples=rdd.filter(_._1.startsWith(domain)).sortBy(_._2,false).take(2)
    println(tuples.toBuffer)
  }




}
