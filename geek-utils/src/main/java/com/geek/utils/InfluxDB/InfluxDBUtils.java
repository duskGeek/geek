package com.geek.utils.InfluxDB;

import java.util.Map;

/**
 *
 * InfluxDB保存
 */
public class InfluxDBUtils {

	public static String getInfluxIP() {
		String ip = "yqdata000"; //修改自己的influxdb的机器IP
		
		Map<String, String> getenv = System.getenv();
		if (getenv.containsKey("INFLUXDB_IP")) {
			ip = getenv.get("INFLUXDB_IP");
		}
		
		return ip;
	}
	
	public static String getRandomMeasurement() {
		return "measurement_" + System.nanoTime();
	}
	
	public static String getInfluxPORT(boolean apiPort) {
		String port = "18086";
		
		Map<String, String> getenv = System.getenv();		
		if(apiPort) {		
			if (getenv.containsKey("INFLUXDB_PORT_API")) 
				port = getenv.get("INFLUXDB_PORT_API");
		}
		else {
			port = "8096";
			if (getenv.containsKey("INFLUXDB_PORT_COLLECTD")) 
				port = getenv.get("INFLUXDB_PORT_COLLECTD");
		}			
		
		return port;
	}

	public static String defaultRetentionPolicy(String version) {
		if (version.startsWith("0.") ) {
			return "default";
		} else {
			return "autogen";
		}
	}

}
