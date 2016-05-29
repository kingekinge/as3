package topURL

/**
  * Created by zhang on 2016/4/13.
  */
object binaryDemo {

  def main(args: Array[String]) {
    val ints: List[Int] = (1 to 10).toList
    val index: Int = binarySearch(ints)
    println(ints(index))

  }


  def  binarySearch(inclusive :List[Int]):Int={
    var start:Int =0;
    var end:Int =inclusive.length-1 //9
    val key :Int=6
    var mid =(start+end)/2

    while ((inclusive(mid)!= key)){
      if(key>inclusive(mid))
        start=mid+1
      else if(key< inclusive(mid))
        end = mid -1

      mid =(start+end)/2
    }
    mid
  }
}
