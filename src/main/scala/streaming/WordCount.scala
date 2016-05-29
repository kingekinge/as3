package streaming

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by zhang on 2016/4/19.
  */
object WordCount extends  App{

  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  //设置DStream批次时间间隔为5秒
  val ssc = new StreamingContext(conf, Seconds(1))
  ssc.checkpoint("c://aaa")
  //监听端口
   val lines = ssc.socketTextStream("192.168.40.180", 9999)

    //不累加
//  private val dStream: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    //累加
   var func =(it:Iterator[(String,Seq[Int],Option[Int])])=>{
     it.flatMap{
       case (x,y,z)=>{
         Some(y.sum+z.getOrElse(0)).map((x,_))
       }
     }
}

  private val dStream: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).updateStateByKey(func,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

  dStream.print()
  ssc.start()
  ssc.awaitTermination()

}
