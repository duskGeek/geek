package com.geek.sparksql.ods.prewarning

import com.geek.sparksql.inter.AppLogging
import com.geek.utils.ContextUtils
import org.apache.spark.sql.SaveMode

object PrewarningETLApp extends AppLogging{
  def main(args: Array[String]): Unit = {
    if (args == null || args.length <= 0) {
      logError("参数为空，请传入输入")

      return 1
    }
    val input = args(0)
    val output = args(1)

    val spark = ContextUtils.getSparkSession(this.getClass.getSimpleName, "")
    val df=spark.read.format("json").load(input)
    //df.printSchema()


    df.write.format("orc").mode(SaveMode.Overwrite).save(output)
    spark.stop()


  }
}
