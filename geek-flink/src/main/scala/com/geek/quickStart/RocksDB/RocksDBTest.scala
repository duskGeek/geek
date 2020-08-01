package com.geek.quickStart.RocksDB

import org.rocksdb.{Options, RocksDB}

object RocksDBTest {

  def main(args: Array[String]): Unit = {
    val option=new Options
    option.setCreateIfMissing(true)

    val rocks=RocksDB.open(option,"data")

    //rocks.put("yq".getBytes("UTF-8"),"nb".getBytes("UTF-8"))
    rocks.delete("yq".getBytes("UTF-8"))
    System.out.println(new String(rocks.get("yq".getBytes("UTF-8"))))

    option.close()
    option.close()

  }

}
