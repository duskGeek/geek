package com.geek.quickStart

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util.Random

object SourceFunctionJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val random= new Random(3)



    System.out.println(random.nextInt(3))
    System.out.println(random.nextInt(3))
    System.out.println(random.nextInt(3))
    System.out.println(random.nextInt(3))



    env.execute(this.getClass.getSimpleName)
  }

}
