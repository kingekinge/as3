package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhang on 2016/4/20.
  */
object SparkSql extends App{
  private val conf: SparkConf = new SparkConf().setAppName("Sql").setMaster("local")
  private val sc: SparkContext = new SparkContext(conf)
  val lineRDD = sc.textFile("c:/table.txt").map(_.split("\\s+"))
  val sqlContext = new SQLContext(sc)


  case class Person(name:String, age:Int, job:String)

  private val personRdd: RDD[Person] = lineRDD.map(x=>Person(x(0),x(1).toInt,x(2)))
  //导入隐式转换toDF
  import  sqlContext.implicits._
  private val personDF: DataFrame = personRdd.toDF()

  //personDF.show()

  //查看DataFrame部分列中的内容
  //personDF.select(personDF.col("name"),personDF.col("age")).show
  //personDF.select("name").show
  //查询所有的name和age，并将age+1
//  personDF.select(personDF("name"), personDF("age") + 1).show

 //personDF.filter(personDF.col("age").>(30)).show()

  //personDF.orderBy("age").show()



}
