package com.geek.sparksql.cdnAccess

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import com.geek.utils.parse.GetIPAddress

object LogParse {

  val cal=Calendar.getInstance()

  def parse(log:AccessLog) :AccessLogAfter={
    val df=new SimpleDateFormat("[d/MMM/yyyy:HH:mm:ss Z]", Locale.UK)
    val time=Option(log.time) match{
      case Some(x)=> Some(x).getOrElse("")
      case None => return null
    }

    try{
    val date=df.parse(time)
    cal.setTime(date)
    val m = cal.get(Calendar.MONTH)
    val d = cal.get(Calendar.DAY_OF_MONTH)
    val year = cal.get(Calendar.YEAR).toString
    val month = String.valueOf(if (m < 10) "0" + m else m)
    val day = String.valueOf(if (d < 10) "0" + d else d)

    val ip=log.accessIp
    val region=GetIPAddress.search(log.accessIp)
    val array=Option(region) match{
      case Some(x)=> {
        val adress = x.split("\\|")
        if (adress != null && adress.length > 0) {
          Array(adress(0),adress(2),adress(3),adress(4))
        }else Array("","","","")
      }
      case None => Array("","","","")
    }

    var country = Some(array(0)).getOrElse("").toString
    var prov = Some(array(1)).getOrElse("").toString
    var city = Some(array(2)).getOrElse("").toString
    var networkOperator = Some(array(3)).getOrElse("").toString

    val urlArray=Option(log.url) match{
      case Some(url)=> {
        val query=if (url.indexOf("?") > 0) url.substring(url.indexOf("?")).substring(1)else ""

        val (protocol,domain,path)=if (url.indexOf("://") > 0) {
          val protocol = url.substring(0, url.indexOf("://")) + "://"
          val domainPath = url.substring(url.indexOf(protocol) + protocol.length, if (url.indexOf("?") > 0) url.
            indexOf("?") else url.length - 1)
          val domain = domainPath.substring(0, domainPath.indexOf("/"))
          val path = domainPath.substring(domainPath.indexOf("/"))
          (protocol,domain,path)
        }
        Array(protocol, domain, path, query)
      }
      case None => Array("","","","")
    }
    val protocol = urlArray(0)
    val domain = urlArray(1)
    val path = urlArray(2)
    val query = urlArray(3)

    AccessLogAfter(ip, log.proxyIp, log.responseTime, log.referer, log.method, log.httpCode, log.requestSize,
      log.responseSize, log.hitCache,log.userAgent, log.fileType, country, prov, city, networkOperator, year, day, month,
      protocol+"", domain+"", path+"", query+"", "")
    }catch {
      case e:NumberFormatException =>println(log); return  null
    }
  }

  def main(args: Array[String]): Unit = {


    parse(AccessLog("[28/Feb/2020:15:37:38 +0800]","139.205.220.39","","1".toInt,"","","http://www.baidu.com/index.html?id=78997","","","","","",""))
  }
}
