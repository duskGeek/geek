package com.geek.sparksql.cdnAccess

import java.sql.PreparedStatement
import java.text.SimpleDateFormat

import com.geek.sparksql.inter.{AppLogging, SparkListenerExpand}
import com.geek.utils.mysql.MysqlConnect
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListenerTaskEnd

class MyListenter(sparkConf: SparkConf) extends SparkListenerExpand with AppLogging{

  var statement: PreparedStatement  = _

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val conn=MysqlConnect.conn()
    conn.setAutoCommit(false)
    statement=conn.prepareStatement("INSERT INTO etl_mocard.spark_listenter_log(app_id,app_name,stage_id,task_id," +
      "task_type,status,log_type,log_value) VALUES(?,?,?,?,?,?,?,?)")

    val appId=sparkConf.getAppId
    val appName=sparkConf.get("spark.app.name")
    println("appID->>>>"+appId+"appName->>>>"+appName)

    val stageId=taskEnd.stageId
    println("######################taskEnd.stageId->>>"+taskEnd.stageId)

    val taskId=taskEnd.taskInfo.taskId
    println("######################taskEnd.taskInfo.taskId->>>"+taskEnd.taskInfo.taskId)

    val taskType=taskEnd.taskType
    println("######################taskEnd.taskType->>>"+taskEnd.taskType)

    val status=taskEnd.taskInfo.status
    println("######################taskEnd.taskInfo.status->>>"+taskEnd.taskInfo.status)

    println("######################taskEnd.taskInfo.failed->>>"+taskEnd.taskInfo.failed)

    val df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println("######################taskEnd.taskMetrics.executorCpuTime->>>>>"+taskEnd.taskMetrics.executorCpuTime/1000000000d/60d)
    insertLog(appName,appId,"taskEnd.taskMetrics.executorCpuTime"
      ,String.valueOf(taskEnd.taskMetrics.executorCpuTime/1000000000d/60d),stageId.toString,taskId.toString,taskType,status);

    println("########################taskEnd.taskMetrics.resultSize->>>>"+taskEnd.taskMetrics.resultSize)
    insertLog(appName,appId,"taskEnd.taskMetrics.resultSize",String.valueOf(taskEnd.taskMetrics.resultSize),
      stageId.toString,taskId.toString,taskType,status)

    println("########################taskEnd.taskMetrics.inputMetrics.bytesRead->>>>"+taskEnd.taskMetrics.inputMetrics.bytesRead/1024/1024)
    insertLog(appName,appId,"taskEnd.taskMetrics.inputMetrics.bytesRead",
      String.valueOf(taskEnd.taskMetrics.inputMetrics.bytesRead/1024/1024)
      ,stageId.toString,taskId.toString,taskType,status)

    println("########################taskEnd.taskMetrics.inputMetrics.recordsRead->>>>"+taskEnd.taskMetrics.inputMetrics.recordsRead)
    insertLog(appName,appId,"taskEnd.taskMetrics.inputMetrics.recordsRead",
      String.valueOf(taskEnd.taskMetrics.inputMetrics.recordsRead)
      ,stageId.toString,taskId.toString,taskType,status)

    println("########################taskEnd.taskMetrics.outputMetrics.bytesWritten->>>>"+taskEnd.taskMetrics.outputMetrics.bytesWritten/1024/1024)
    insertLog(appName,appId,"taskEnd.taskMetrics.outputMetrics.bytesWritten",
      String.valueOf(taskEnd.taskMetrics.outputMetrics.bytesWritten/1024/1024),
      stageId.toString,taskId.toString,taskType,status)

    println("########################taskEnd.taskMetrics.outputMetrics.recordsWritten->>>>"+taskEnd.taskMetrics.outputMetrics.recordsWritten)
    insertLog(appName,appId,"taskEnd.taskMetrics.outputMetrics.recordsWritten",
      String.valueOf(taskEnd.taskMetrics.outputMetrics.recordsWritten),
      stageId.toString,taskId.toString,taskType,status)

    println("########################taskEnd.taskMetrics.shuffleReadMetrics.recordsRead->>>>"+taskEnd.taskMetrics.shuffleReadMetrics.recordsRead)
    insertLog(appName,appId,"taskEnd.taskMetrics.shuffleReadMetrics.recordsRead",
      String.valueOf(taskEnd.taskMetrics.shuffleReadMetrics.recordsRead),
      stageId.toString,taskId.toString,taskType,status)

    println("########################taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten->>>>"+taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten)
    insertLog(appName,appId,"taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten",
      String.valueOf(taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten),
      stageId.toString,taskId.toString,taskType,status)

    statement.executeBatch()
    conn.commit()
  }

  def insertLog( appName:String,appId:String,logType:String,logVales:String
                 ,stageId:String,taskId:String ,taskType:String,status:String): Unit = {
    if(statement==null){
      return
    }

    //app_id,app_name,stage_id,task_id,task_type,status,log_type,log_value
    statement.setString(1,appId)
    statement.setString(2,appName)
    statement.setString(3,stageId)
    statement.setString(4,taskId)
    statement.setString(5,taskType)
    statement.setString(6,status)
    statement.setString(7,logType)
    statement.setString(8,logVales)

    statement.addBatch()
  }
}
