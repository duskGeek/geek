package com.geek.utils.parse;

import org.lionsoul.ip2region.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class GetIPAddress {

    static String dbFile=GetIPAddress.class.getClassLoader().getResource("ip2region.db").getFile();
//    static String dbFile= SparkFiles.get("ip2region.db");

    public static String search(String ip){
        return  search(ip,"");
    }


    public static String search(String ip,String searchType){

        int algorithm = DbSearcher.BTREE_ALGORITHM;
        String algoName = "B-tree";

        if ( searchType.equalsIgnoreCase("binary")) {
            algoName  = "Binary";
            algorithm = DbSearcher.BINARY_ALGORITHM;
        } else if ( searchType.equalsIgnoreCase("memory") ) {
            algoName  = "Memory";
            algorithm = DbSearcher.MEMORY_ALGORITYM;
        }

        try {
            DbConfig config = new DbConfig();
            DbSearcher searcher = new DbSearcher(config, dbFile);

            //define the method
            Method method = null;
            switch ( algorithm )
            {
                case DbSearcher.BTREE_ALGORITHM:
                    method = searcher.getClass().getMethod("btreeSearch", String.class);
                    break;
                case DbSearcher.BINARY_ALGORITHM:
                    method = searcher.getClass().getMethod("binarySearch", String.class);
                    break;
                case DbSearcher.MEMORY_ALGORITYM:
                    method = searcher.getClass().getMethod("memorySearch", String.class);
                    break;
            }

            double sTime = 0, cTime = 0;

            DataBlock dataBlock = null;

            if ( Util.isIpAddress(ip) == false ) {
                System.out.println("Error: Invalid ip address "+ip);
                return "";
            }

            sTime = System.nanoTime();
            dataBlock = (DataBlock) method.invoke(searcher, ip);
            cTime = (System.nanoTime() - sTime) / 1000000;

            searcher.close();
            return dataBlock.getRegion();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (DbMakerConfigException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return  null;
    }

    public static void main(String[] args) {
        //System.out.println(GetIPAddress.dbFile);
        System.out.println(GetIPAddress.search("139.205.220.39"));
    }


}
