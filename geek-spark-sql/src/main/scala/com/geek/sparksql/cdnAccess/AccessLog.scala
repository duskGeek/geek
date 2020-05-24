package com.geek.sparksql.cdnAccess

case class AccessLog(time:String
                     ,accessIp:String
                     ,proxyIp:String
                     ,responseTime:Int
                     ,referer:String
                     ,method:String
                     ,url:String
                     ,httpCode:String
                     ,requestSize:String
                     ,responseSize:String
                     ,hitCache:String
                     ,userAgent:String
                     ,fileType:String )
