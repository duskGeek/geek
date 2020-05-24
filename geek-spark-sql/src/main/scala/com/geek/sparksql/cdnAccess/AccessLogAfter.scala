package com.geek.sparksql.cdnAccess

case class AccessLogAfter(
   accessIp:String
   ,proxyIp:String
   ,responseTime:Int
   ,referer:String
   ,method:String
   ,httpCode:String
   ,requestSize:String
   ,responseSize:String
   ,hitCache:String
   ,userAgent:String
   ,fileType:String
   ,country:String
   ,prov:String
   ,city:String
   ,networkOperator:String
   ,year:String
   ,day:String
   ,month:String
   ,protocol:String
   ,domain:String
   ,path:String
   ,query:String
   ,userId:String
){
  override def toString: String = accessIp + '\t' + proxyIp + '\t' + responseTime + '\t' + referer + '\t' + method +
    '\t' + httpCode + '\t' + requestSize + '\t' + responseSize + '\t' + hitCache + '\t' + userAgent + '\t' + fileType +
    '\t' + country + '\t' + prov + '\t' + city + '\t' + networkOperator + '\t' + year + '\t' + month + '\t' + day +
    '\t' + protocol + '\t' + domain + '\t' + path + '\t' + query + '\t' + userId
}
