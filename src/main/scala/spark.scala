import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.Schedulable
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by zhang on 2016/5/5.
  */
object spark extends App{

  //算pv
  val conf = new SparkConf().setAppName("uv").setMaster("local")//本地模式并使用两个线程
  val sc = new SparkContext(conf)

   val map = sc.textFile("c:/uv.txt",3)//.map(x=>{
//    val a=x.split("\\s+")
//    (a(0),a(1))
//  }).distinct().map(x=>(x._1,1)).reduce((x,y)=>x._2+y._2)
//  println(map)

//  val p= sc.textFile("C:\\Users\\zhang\\Desktop\\README.md").flatMap(_.toCharArray)
//    .filter(x=>(x>='A'&&x<='Z'||x>='a'&&x<='z'))
//  .map((_,1)).reduceByKey(_+_)
//  print(p.collect().toBuffer)

//  val r=sc.textFile("C:\\Users\\zhang\\Desktop\\1.txt").map(x=>{
//     val split = x.split(",")
//     (split(0),(split(1).toInt,1))
//   }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>(x._1,BigDecimal(x._2._1)./(BigDecimal(x._2._2))))
//
//  val n=sc.textFile("C:\\Users\\zhang\\Desktop\\2.txt").map(x=>{
//    val split = x.split(",")
//    (split(0),split(1))
//  })
//
//  val o=r.join(n).map(x=>(x._1,x._2._2,x._2._1))

//  println(o.collect().toBuffer)

private val rdd: RDD[String] = sc.textFile("hdfs://192.168.40.102:9000/wordcount/input")
  private val buffer: mutable.Buffer[(String, Int)] = rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().toBuffer
  println(buffer)
}


