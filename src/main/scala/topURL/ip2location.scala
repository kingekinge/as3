package topURL


import java.util

import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhang on 2016/4/13.
  */
object ip2location {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("ip2location").setMaster("local")
    val sc = new SparkContext(conf)

    val arry = sc.textFile("C:/ip.txt").map(k => {
      val split = k.split("\\|")
      (split(2).toLong, split(3).toLong, split(6))
    }).collect()

    val broadcast: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(arry)

    sc.textFile("C:/20090121000132.394251.http.format").map(k => {
      val split = k.split("\\|")
      val ip = split(1)
      ip
    }).mapPartitions(k => {
      val array: Array[(Long, Long, String)] = broadcast.value
      k.map(v=>{
        val ip = ip2Long(v)
        val index=binarySerachAddr(array,ip)
        (array(index)._3,1)
      })
    }).reduceByKey(_+_).foreachPartition(mysql2data(_))
    sc.stop()


  }

  def mysql2data(iterator: Iterator[(String, Int)]) ={
    var conn: Connection = null
    var ps : PreparedStatement = null
    val sql = "INSERT INTO location_info (location, counts, accesse_date) VALUES (?, ?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://192.168.40.101:3306/mysql", "root", "root")
      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println("Mysql Exception:"+e)
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }

  /**
    * 二分查找ip地址
    *
    * @param array
    * @return
    */
  def binarySerachAddr(array : Array[(Long,Long,String)],ip:Long): Int ={
    var start =0;
    var end =array.length-1
    var mid =(start+end)/2

    while(!(ip>=array(mid)._1&& ip <= array(mid)._2 ) ){
      if(ip < array(mid)._1) {
        end = mid - 1
      }else {
        start=mid+1
      }
      mid=(start+end)/2
    }
    mid
  }

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
}
