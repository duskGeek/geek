package com.geek.app

import java.lang
import java.sql.Timestamp

import com.alibaba.fastjson.{JSON, JSONObject}
import com.geek.DAO.CatagoryDAO
import com.geek.quickStart.KafkaOperatorApp.CreateKafkaSource
import org.apache.commons.collections.IteratorUtils
import org.apache.flink.api.common.functions.{CoGroupFunction, JoinFunction, RichMapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object DoubleJoinStreamApp {

//  {"database":"yqdata","table":"order1","type":"insert","ts":1607255275,"xid":1016,"commit":true,"data":{"id":9,"order_id":"order200","money":11.00,"catagory":2}}
//  {"database":"yqdata","table":"order1","type":"update","ts":1607255292,"xid":1041,"commit":true,"data":{"id":9,"order_id":"order200","money":22.00,"catagory":2},"old":{"money":11.00}}
//  {"database":"yqdata","table":"order1","type":"delete","ts":1607255312,"xid":1071,"commit":true,"data":{"id":9,"order_id":"order200","money":22.00,"catagory":2}}
//  {"database":"yqdata","table":"dept","type":"insert","ts":1607255461,"xid":1278,"commit":true,"data":{"dept_name":"BWM","num":10}}
//  {"database":"yqdata","table":"dept","type":"update","ts":1607255484,"xid":1313,"commit":true,"data":{"dept_name":"BWM","num":20},"old":{"num":10}}
//  {"database":"yqdata","table":"dept","type":"update","ts":1607255492,"xid":1327,"commit":true,"data":{"dept_name":"jiebao","num":20},"old":{"num":1}}
//

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
     val tool=ParameterTool.fromPropertiesFile("conf/flink-job.properties")

    val kafkaSorce=new CreateKafkaSource(env)
    val maxWellStream=kafkaSorce.create(tool)

    val orderStream=maxWellStream.filter(_.contains("\"table\":\"order"))

    val catagoryStream=maxWellStream.filter(_.contains("\"table\":\"catagory"))


    val orderProcessStream=orderStream.process(new ProcessFunction[String,Order] {
      override def processElement(value: String, ctx: ProcessFunction[String, Order]#Context, out: Collector[Order]): Unit = {
        try {
        val orderJson=JSON.parseObject(value).getJSONObject("data")
        val o=Order(orderJson.getInteger("id"),
        orderJson.getString("order_id"),
        orderJson.getBigDecimal("money"),
        orderJson.getInteger("catagory"),
        orderJson.getLong("order_time"),"")
        out.collect(o)
        }catch {
          case e=>e.printStackTrace()
        }
      }
    }) .assignTimestampsAndWatermarks(new MyPriodAssignTimestampAndWatermarksOrder(2000))
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(0)) {
//      override def extractTimestamp(element: Order): Long = {
//        element.orderTime
//      }
//    })


    val catagoryProcessStream=catagoryStream.process(new ProcessFunction[String,Catagory]{
      override def processElement(value: String, ctx: ProcessFunction[String, Catagory]#Context, out: Collector[Catagory]): Unit = {
        try {
          val catagoryJson=JSON.parseObject(value).getJSONObject("data")
          val catgory=Catagory(catagoryJson.getInteger("catagory_id"),
            catagoryJson.getString("catagory_name"),
            catagoryJson.getLong("catagory_time"))
          out.collect(catgory)
        }catch {
          case e=>e.printStackTrace()
        }
      }
    }) .assignTimestampsAndWatermarks(new MyPriodAssignTimestampAndWatermarksCatagory(2000))
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Catagory](Time.seconds(0)) {
//      override def extractTimestamp(element: Catagory): Long = {
//        element.catagoryTime
//      }
//    })

    orderProcessStream.print()
    catagoryProcessStream.print()


    //joinStream(env,orderProcessStream,catagoryProcessStream)
    //coGroupSteamOut(env,orderProcessStream,catagoryProcessStream)
    leftLateJoinStram(env,orderProcessStream,catagoryProcessStream)

    env.execute(this.getClass.getSimpleName)

  }


  def joinStream(env:StreamExecutionEnvironment,orderProcessStream: DataStream[Order],
                 catagoryProcessStream : DataStream[Catagory] ): Unit ={
    orderProcessStream.join(catagoryProcessStream).where(o=>{ o.catagoryId}).equalTo(cat=>{ cat.catagoryId}).
      window(TumblingEventTimeWindows.of(Time.seconds(10))).apply(new JoinFunction[Order,Catagory,Order] {
      override def join(first: Order, second: Catagory): Order = {
        first.copy(catagoryName = second.catagoryName)
      }
    }).print("===>")

  }

  def coGroupSteamOut(env:StreamExecutionEnvironment,orderProcessStream: DataStream[Order],
                   catagoryProcessStream : DataStream[Catagory] ): Unit ={

    coGroupSteam(env,orderProcessStream,catagoryProcessStream).print("==>")

  }

  def coGroupSteam(env:StreamExecutionEnvironment,orderProcessStream: DataStream[Order],
                   catagoryProcessStream : DataStream[Catagory] ):  DataStream[Order] ={

    orderProcessStream.coGroup(catagoryProcessStream).where(o=>{ o.catagoryId}).equalTo(cat=>{ cat.catagoryId}).
      window(TumblingEventTimeWindows.of(Time.seconds(10))).apply(new CoGroupFunction[Order,Catagory,Order] {
      override def coGroup(first: lang.Iterable[Order], second: lang.Iterable[Catagory], out: Collector[Order]): Unit = {
        val orderIt=first.iterator()
        while (orderIt.hasNext){
          val order=orderIt.next()
          if(second.iterator().hasNext){
            val catagoryIt=second.iterator()
            while (catagoryIt.hasNext){
              val catagory=catagoryIt.next()
              out.collect(order.copy(catagoryName = catagory.catagoryName))
            }
          }else{
            out.collect(order)
          }
        }
      }
    })
  }

  def leftLateJoinStram(env:StreamExecutionEnvironment,orderProcessStream: DataStream[Order],
                        catagoryProcessStream : DataStream[Catagory] ): Unit ={

    val lateOrderOutPutTag=new OutputTag[Order]("order_late")
    val orderAddTagStream=orderProcessStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).
      sideOutputLateData(lateOrderOutPutTag).apply(new AllWindowFunction[Order,Order,TimeWindow] {
      override def apply(window: TimeWindow, input: Iterable[Order], out: Collector[Order]): Unit = {
        for(order <- input){
          out.collect(order)
        }
      }
    })

    val lateDataStream=orderAddTagStream.getSideOutput(lateOrderOutPutTag).map(new RichMapFunction[Order,Order] {
      var catagoryDAO :CatagoryDAO= _
      override def open(parameters: Configuration): Unit = {
        catagoryDAO=new CatagoryDAO
      }

      override def map(value: Order): Order = {
        val catagoryName=catagoryDAO.findById(value.catagoryId)
        value.copy(catagoryName=catagoryName+"Late")
      }
    })

    val unionOrder=coGroupSteam(env,orderProcessStream,catagoryProcessStream).union(lateDataStream)


    ///如果右边延迟，过滤后的数据都被存放在同一个滚动窗口里，等待窗口结束触发再去数据库里查一次，
    // 不过靠近窗口结束边缘的数据没有充分的时间等待，放在窗口里等待的时间是每条数据是不一样的，所以使用滑动窗口，反复去数据库中查询
    // 可以输出到其它地方使用其它程序再处理
    val rightLateStream=unionOrder.filter(_.catagoryName=="")
      //.windowAll(TumblingEventTimeWindows.of(Time.seconds(40))).
        .windowAll(SlidingEventTimeWindows.of(Time.seconds(40),Time.seconds(10))).
      process(new ProcessAllWindowFunction[Order,Order,TimeWindow] {
        var catagoryDAO :CatagoryDAO= _
        override def open(parameters: Configuration): Unit = {
          catagoryDAO=new CatagoryDAO
        }

        override def process(context: Context, elements: Iterable[Order], out: Collector[Order]): Unit = {
          for(order <- elements){
            val catagoryName=catagoryDAO.findById(order.catagoryId)
            out.collect(order.copy(catagoryName=catagoryName+"RightLate"))
          }
        }
      })
    unionOrder.union(rightLateStream).print("====>")

  }


}

@SerialVersionUID(value = 19999999999999999L)
case class Order(id:Int,orderId:String,money:BigDecimal,catagoryId:Int,orderTime:Long,catagoryName:String)

@SerialVersionUID(value = 19999999999999999L)
case class Catagory(catagoryId:Int,catagoryName:String,catagoryTime:Long)



class MyPriodAssignTimestampAndWatermarksCatagory(allowDelayTime:Long) extends AssignerWithPeriodicWatermarks[Catagory]{

  var maxTimeStamp= 0L

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTimeStamp-allowDelayTime)
  }

  override def extractTimestamp(element: Catagory, previousElementTimestamp: Long): Long = {
    val nowTime=element.catagoryTime
    maxTimeStamp=maxTimeStamp.max(nowTime)
    println("nowTime: "+new Timestamp(nowTime) +
      " previousTime"+new Timestamp(previousElementTimestamp)+
      " maxTimeStamp"+new Timestamp(maxTimeStamp)+
      " Watermark"+getCurrentWatermark.getTimestamp)

    nowTime
  }
}



class MyPriodAssignTimestampAndWatermarksOrder(allowDelayTime:Long) extends AssignerWithPeriodicWatermarks[Order]{

  var maxTimeStamp= 0L

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTimeStamp-allowDelayTime)
  }

  override def extractTimestamp(element: Order, previousElementTimestamp: Long): Long = {
    val nowTime=element.orderTime
    maxTimeStamp=maxTimeStamp.max(nowTime)
    println("nowTime: "+new Timestamp(nowTime) +
      " previousTime"+new Timestamp(previousElementTimestamp)+
      " maxTimeStamp"+new Timestamp(maxTimeStamp)+
      " Watermark"+getCurrentWatermark.getTimestamp)

    nowTime
  }
}

//
//
//INSERT INTO `catagory` VALUES ('1', '1', '图书', '2020-12-09 23:17:54', '2020-12-13 01:25:52', '1000');
//INSERT INTO `catagory` VALUES ('2', '2', '生活用品', '2020-12-13 01:22:40', '2020-12-13 01:22:40', '15000');
//INSERT INTO `catagory` VALUES ('4', '4', '家电', '2020-12-13 01:23:33', '2020-12-13 01:23:33', '35000');
//INSERT INTO `catagory` VALUES ('5', '5', '酒水', '2020-12-13 01:23:33', '2020-12-13 01:23:33', '45000');
//INSERT INTO `catagory` VALUES ('3', '3', '食品', '2020-12-13 01:23:33', '2020-12-13 01:23:33', '22000');
//INSERT INTO `catagory` VALUES ('6', '6', '生鲜', '2020-12-13 01:23:33', '2020-12-13 01:23:33', '55000');
//INSERT INTO `catagory` VALUES ('7', '7', '水果', '2020-12-13 01:23:33', '2020-12-13 01:23:33', '65000');
//INSERT INTO `catagory` VALUES ('8', '8', '百货', '2020-12-13 01:23:33', '2020-12-13 01:23:33', '85000');
//
//
//
//INSERT INTO `order1` VALUES ('6', 'order100', '55.55', '1', '2020-12-06 23:00:46', '2020-12-13 01:23:01', '2000');
//INSERT INTO `order1` VALUES ('7', 'order110', '11.55', '1', '2020-12-06 23:00:46', '2020-12-13 01:13:39', '4200');
//INSERT INTO `order1` VALUES ('8', 'order120', '10.00', '2', '2020-12-09 23:26:46', '2020-12-09 23:27:18', '18000');
//INSERT INTO `order1` VALUES ('10', 'order140', '20.00', '3', '2020-12-09 23:39:06', '2020-12-13 01:23:44', '25100');
//INSERT INTO `order1` VALUES ('9', 'order130', '10.00', '2', '2020-12-09 23:26:46', '2020-12-09 23:27:18', '19000');
//INSERT INTO `order1` VALUES ('11', 'order150', '50.00', '3', '2020-12-09 23:45:19', '2020-12-13 01:21:25', '35000');
//INSERT INTO `order1` VALUES ('12', 'order160', '10.00', '4', '2020-12-10 01:25:35', '2020-12-13 01:21:18', '41000');
//INSERT INTO `order1` VALUES ('13', 'order170', '10.00', '4', '2020-12-10 01:25:35', '2020-12-13 01:21:18', '42000');
//INSERT INTO `order1` VALUES ('14', 'order180', '20.00', '2', '2020-12-10 01:25:35', '2020-12-13 01:21:18', '15000');
//INSERT INTO `order1` VALUES ('15', 'order190', '10.00', '6', '2020-12-10 01:25:35', '2020-12-13 01:21:18', '52000');
//INSERT INTO `order1` VALUES ('16', 'order1000', '10.00', '4', '2020-12-10 01:25:35', '2020-12-13 01:21:18', '62000');
//INSERT INTO `order1` VALUES ('17', 'order1100', '10.00', '4', '2020-12-10 01:25:35', '2020-12-13 01:21:18', '82000');
//INSERT INTO `order1` VALUES ('18', 'order1200', '10.00', '4', '2020-12-10 01:25:35', '2020-12-13 01:21:18', '92000');



//日志：
/*
nowTime: 1970-01-01 08:00:01.0 previousTime2020-12-13 23:54:44.13 maxTimeStamp1970-01-01 08:00:01.0 Watermark-1000
Catagory(1,图书,1000)
nowTime: 1970-01-01 08:00:15.0 previousTime2020-12-13 23:54:44.168 maxTimeStamp1970-01-01 08:00:15.0 Watermark13000
Catagory(2,生活用品,15000)
nowTime: 1970-01-01 08:00:35.0 previousTime2020-12-13 23:54:44.207 maxTimeStamp1970-01-01 08:00:35.0 Watermark33000
Catagory(4,家电,35000)
nowTime: 1970-01-01 08:00:45.0 previousTime2020-12-13 23:54:44.249 maxTimeStamp1970-01-01 08:00:45.0 Watermark43000
Catagory(5,酒水,45000)
nowTime: 1970-01-01 08:00:02.0 previousTime2020-12-13 23:54:49.925 maxTimeStamp1970-01-01 08:00:02.0 Watermark0
Order(6,order100,55.55,1,2000,)
nowTime: 1970-01-01 08:00:04.2 previousTime2020-12-13 23:54:49.965 maxTimeStamp1970-01-01 08:00:04.2 Watermark2200
Order(7,order110,11.55,1,4200,)
nowTime: 1970-01-01 08:00:18.0 previousTime2020-12-13 23:54:50.003 maxTimeStamp1970-01-01 08:00:18.0 Watermark16000
Order(8,order120,10.00,2,18000,)
====>> Order(6,order100,55.55,1,2000,图书)
====>> Order(7,order110,11.55,1,4200,图书)
nowTime: 1970-01-01 08:00:25.1 previousTime2020-12-13 23:54:53.991 maxTimeStamp1970-01-01 08:00:25.1 Watermark23100
Order(10,order140,20.00,3,25100,)
====>> Order(8,order120,10.00,2,18000,生活用品)
nowTime: 1970-01-01 08:00:19.0 previousTime2020-12-13 23:55:20.806 maxTimeStamp1970-01-01 08:00:25.1 Watermark23100
Order(9,order130,10.00,2,19000,)
====>> Order(9,order130,10.00,2,19000,生活用品Late)
nowTime: 1970-01-01 08:00:35.0 previousTime2020-12-13 23:55:31.959 maxTimeStamp1970-01-01 08:00:35.0 Watermark33000
Order(11,order150,50.00,3,35000,)
====>> Order(10,order140,20.00,3,25100,)
====>> Order(10,order140,20.00,3,25100,RightLate)
nowTime: 1970-01-01 08:00:41.0 previousTime2020-12-13 23:55:50.092 maxTimeStamp1970-01-01 08:00:41.0 Watermark39000
Order(12,order160,10.00,4,41000,)
nowTime: 1970-01-01 08:00:42.0 previousTime2020-12-13 23:56:02.537 maxTimeStamp1970-01-01 08:00:42.0 Watermark40000
Order(13,order170,10.00,4,42000,)
====>> Order(11,order150,50.00,3,35000,)
====>> Order(10,order140,20.00,3,25100,RightLate)
====>> Order(11,order150,50.00,3,35000,RightLate)
nowTime: 1970-01-01 08:00:22.0 previousTime2020-12-13 23:56:14.094 maxTimeStamp1970-01-01 08:00:45.0 Watermark43000
Catagory(3,食品,22000)
nowTime: 1970-01-01 08:00:15.0 previousTime2020-12-13 23:56:16.97 maxTimeStamp1970-01-01 08:00:42.0 Watermark40000
Order(14,order180,20.00,2,15000,)
====>> Order(14,order180,20.00,2,15000,生活用品Late)
nowTime: 1970-01-01 08:00:52.0 previousTime2020-12-13 23:56:20.088 maxTimeStamp1970-01-01 08:00:52.0 Watermark50000
Order(15,order190,10.00,4,52000,)      ////wm取两条流最小值，左流wm（现在是5000）过了窗口时间但是右流还是4000不会触发，等待触发
nowTime: 1970-01-01 08:01:02.0 previousTime2020-12-14 00:12:05.662 maxTimeStamp1970-01-01 08:01:02.0 Watermark60000
Order(16,order1000,10.00,4,62000,)
nowTime: 1970-01-01 08:01:22.0 previousTime2020-12-14 00:13:09.282 maxTimeStamp1970-01-01 08:01:22.0 Watermark80000
Order(17,order1100,10.00,4,82000,)
nowTime: 1970-01-01 08:00:55.0 previousTime2020-12-14 00:15:44.96 maxTimeStamp1970-01-01 08:00:55.0 Watermark53000
Catagory(6,生鲜,55000)        /////左流等待触发中，右边只要过来任意一条就会触发
====>> Order(12,order160,10.00,4,41000,)   ///触发了，取两条流最小的wm来触发相应窗口
====>> Order(13,order170,10.00,4,42000,)
nowTime: 1970-01-01 08:01:05.0 previousTime2020-12-14 00:29:20.484 maxTimeStamp1970-01-01 08:01:05.0 Watermark63000
Catagory(7,水果,65000)      ////左边一旦触发了之后，右边再怎么来数据都不会触发，只有等左边数据过来触发计算
nowTime: 1970-01-01 08:01:25.0 previousTime2020-12-14 00:31:34.027 maxTimeStamp1970-01-01 08:01:25.0 Watermark83000
Catagory(8,百货,85000)
nowTime: 1970-01-01 08:01:32.0 previousTime2020-12-14 00:31:53.753 maxTimeStamp1970-01-01 08:01:32.0 Watermark90000
Order(18,order1200,10.00,4,92000,) //左边来了一条触发了
====>> Order(12,order160,10.00,4,41000,)
====>> Order(13,order170,10.00,4,42000,)
====>> Order(15,order190,10.00,4,52000,)
====>> Order(16,order1000,10.00,4,62000,)
====>> Order(10,order140,20.00,3,25100,食品RightLate)
====>> Order(11,order150,50.00,3,35000,食品RightLate)
====>> Order(12,order160,10.00,4,41000,家电RightLate)
====>> Order(13,order170,10.00,4,42000,家电RightLate)
====>> Order(10,order140,20.00,3,25100,食品RightLate)
====>> Order(11,order150,50.00,3,35000,食品RightLate)
====>> Order(12,order160,10.00,4,41000,家电RightLate)
====>> Order(13,order170,10.00,4,42000,家电RightLate)
====>> Order(15,order190,10.00,4,52000,家电RightLate)
====>> Order(11,order150,50.00,3,35000,食品RightLate)
====>> Order(12,order160,10.00,4,41000,家电RightLate)
====>> Order(13,order170,10.00,4,42000,家电RightLate)
====>> Order(15,order190,10.00,4,52000,家电RightLate)
====>> Order(16,order1000,10.00,4,62000,家电RightLate)
====>> Order(12,order160,10.00,4,41000,家电RightLate)
====>> Order(13,order170,10.00,4,42000,家电RightLate)
====>> Order(15,order190,10.00,4,52000,家电RightLate)
====>> Order(16,order1000,10.00,4,62000,家电RightLate)
*/