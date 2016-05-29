import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AkkaUtils
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.immutable.Range.Inclusive
import scala.collection.mutable
import scala.math.Ordered

/**
  * Created by zhang on 2016/4/10.
  */
object main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[3]")//本地模式并使用两个线程
    val sc = new SparkContext(conf)
//    val file: File = new File("c:/wc/out")
// sc.textFile("C:/input/wordcount/").repartition(3).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 1).saveAsTextFile(file.getAbsolutePath)


    //    println(tuples.toBuffer)

//
//  val array: Array[Int] = sc.parallelize(Array(1,2,3,4,5)).map(_*21).collect()
//    println(array.toBuffer)
//
//    val rdd5 = sc.parallelize(List(List("a b c", "a b b"),List("e f g", "a f g"), List("h i j", "a a b")))
//    val array1: Array[String] = rdd5.flatMap(_.flatMap(_.split(" "))).collect()
//    println(array1.toBuffer)
//
//
//    //union求并集，注意类型要一致
//    val rdd6 = sc.parallelize(List(5,6,4,7))
//    val rdd7 = sc.parallelize(List(1,2,3,4))
//
//     val array2: Array[Int] = rdd6.union(rdd7).collect()
//    println(array2.toBuffer)

    //intersection求交集
//    val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 2), ("kitty", 3)))
//    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 8), ("shuke", 7)))
//    val array3: Array[(String, Int)] = rdd1.intersection(rdd2).collect()
//    println(array3.toBuffer)

    //#join
//    val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 2), ("kitty", 3)))
//    val rdd2 = sc.parallelize(List(("jerry", 9), ("tom", 8), ("shuke", 7)))
//
//    val rdd3 = rdd1.join(rdd2).collect()
//    val rdd4 = rdd1.leftOuterJoin(rdd2).collect()
//    val rdd5 = rdd1.rightOuterJoin(rdd2).collect()
//
//    println(rdd5.toBuffer)
//
//    val rdd6 = sc.parallelize(List(("jerry", 9), ("tom", 8), ("shuke", 7),("jerry", 19)))
//     val sum: Any = rdd6.reduceByKey(_+_).map((_._2)).sum()
//        println(sum)

//    cogroup
//    val rdd1 = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
//    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
//    val rdd3 = rdd1.cogroup(rdd2).collect()
//    val map: Array[(String, Int)] = rdd3.map(t=>(t._1, t._2._1.sum + t._2._2.sum))
//    println(rdd3.toBuffer)

//    cartesian笛卡尔积
//    val rdd1 = sc.parallelize(List("tom", "jerry"))
//    val rdd2 = sc.parallelize(List("tom", "kitty", "shuke"))
//    val collect: Array[(String, String)] = rdd1.cartesian(rdd2).collect()
//    println(collect.toBuffer)
//
//    val rdd8 = sc.parallelize(List(17,9,3,4,5), 21)
//    println(rdd8.partitions.length)//partition大小
//    println(rdd8.count())//集合长度
//    println(rdd8.top(3).toBuffer)//最大的3个数
//    println(rdd8.take(2).toBuffer)//前面两个数
//    println(rdd8.takeOrdered(rdd8.count.asInstanceOf[Int]).toBuffer)//排序


//    val rdd9 = sc.parallelize(List(17,9,3,4,5,2,1))
//    val array: Array[Array[Int]] = rdd9.glom().collect()
    //    println(array(0).toBuffer)
    //    println(array(1).toBuffer)

//    val rdd10 = sc.parallelize(List(17,9,3,4,5,2,1))
//    val treeReduce: Int = rdd10.treeReduce((x,y)=> x+y)
//    println(treeReduce)

    val array=Array((1,2),(3,8),(3,9))
    val rdd1: RDD[(Int, Int)] = sc.parallelize(array)
    // val a=rdd.reduceByKey(_+_).collect().toBuffer
    //val toBuffer= rdd.mapValues(x=>x to 5).collect().toBuffer
    val rdd2: RDD[(Int, Int)] = sc.parallelize(Array((1,9)))
    //以key匹配，返回只在主rdd出现不包含otherRDD的元素
  }

}

