import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhang on 2016/4/12.
  */
object MobileLocation {

  def main(args: Array[String]) {
    val sparkConf: SparkConf = new SparkConf().setAppName("计算常用基站").setMaster("local")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    //reduceByKey  (K,V)对的集合

    val ap=sparkContext.textFile("c:/spark_data/bs_log").map(v=>{
      val arry=v.split(",")
      val phone=arry(0)
      val time =arry(1).toLong
      val key = phone+"#"+arry(2)
      //标记等于1代表断开连接
      if(arry(3).equals("1")) (key,-time) else (key, time)
                              //根据时间倒排序
    }).reduceByKey(_+_).sortBy(_._2,false).groupBy(_._1.split("#")(0)).map(v=>{
      val phone=v._1
      //取出时间最大的元素进行切割vb
      val id=v._2.toList(0)._1.split("#")(1)
      val time=v._2.toList(0)._2
      (id ,(phone,time))
    })

//    println(ap.collect().toBuffer)

    val sp=sparkContext.textFile("C:/spark_data/lac_info.txt").map(v=> {
      val sp=v.split(",")
      val id =sp(0)
      val x =sp(1)
      val y =sp(2)
        //(基站id，(经度，纬度))
      (id,(x,y))
    })

    //格式化输出
    val pd=ap.join(sp).map(v=>{
      val id=v._1//基站id
      val phone=v._2._1._1//手机
      val time=v._2._1._2//时间
      val x=v._2._2._1//经度
      val y =v._2._2._2 //纬度

      (id,phone,time,x,y)
    })
    println(pd.collect.toBuffer)











  }
}
