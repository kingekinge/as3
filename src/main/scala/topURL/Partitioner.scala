package topURL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by zhang on 2016/4/13.
  */
object Partitioner extends App{

  val conf = new SparkConf().setAppName("WordCount").setMaster("local")
  val sc = new SparkContext(conf)

  private val partitioner: HostPartitioner = new HostPartitioner
  private val rdd: RDD[String] = sc.parallelize(Array("a","b","c","a"),3)
  rdd.map((_,1)).partitionBy(partitioner).foreachPartition(k=>{
    println(k.toBuffer)
  })

}

class HostPartitioner extends Partitioner {

  val partMap =  mutable.HashMap(("a",0),("b",1),("c",2))

  override def numPartitions: Int = partMap.size

  override def getPartition(key: Any): Int = {
    partMap.getOrElse(key.toString, 0)
  }
}
