package com.geek.quickStart.partitionCustom

import com.geek.quickStart.SourceFunctions.Access
import org.apache.flink.api.common.functions.Partitioner

class MyPartition extends Partitioner[String]{
  override def partition(key: String, numPartitions: Int): Int = {
      if(key=="www.baidu.com"){
        0
      }else if(key == "www.taobao.com"){
        1
      }else{
        2
      }
  }
}
