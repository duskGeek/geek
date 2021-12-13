package algorithm

import scala.collection.mutable

object ArrayTest {
  def main(args: Array[String]): Unit = {
    var dis:Set[Int]=Set(1,2,3,1)
    println(maxSubArray(Array(8,-19,5,-4,20)))

  }


  def containsDuplicate(nums: Array[Int]): Boolean = {
    var dis:mutable.Set[Int] =new mutable.HashSet[Int]()
    nums.foreach( dis+=_ )

    nums.foreach(println(_))
    println(dis)
    if(dis.size==nums.length){
      false
    }else{
      true
    }
  }


  def containsDuplicate2(nums: Array[Int]): Boolean = {
    import scala.collection.mutable

    var dis:mutable.Set[Int] =new mutable.HashSet[Int]()

    var flag:Boolean=false
    var breakf:Boolean=false

    val i=nums.iterator
    while (!breakf){
      if(i.hasNext){
        val diff=i.next()
        if(dis.contains(diff)){
          flag=true
          breakf=true
        }
        dis +=diff
      }else{
        breakf=true
      }
    }
    flag
  }

  //array: dp[-2,1,1,4,4,5,6,6,6  ]
  //max( max(pre+arr[i],arr[i]) ,dp[i-1] )   (max(dp[i-1]+arr[i],arr[i]))
  def maxSubArray(nums: Array[Int]): Int = {
    var maxArraysum:Int=0
    var preNum:Int=0
    var index=0

    for (num <- nums){
      if(index==0){
        maxArraysum=num
        preNum=num
      }else{
        preNum=Math.max(preNum+num,num)

        println(preNum," ",num," ",maxArraysum)
        maxArraysum=Math.max(preNum,maxArraysum)
        println(maxArraysum)
      }
      index+=1
    }
    maxArraysum
  }
}
