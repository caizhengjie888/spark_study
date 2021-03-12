package com.kfk.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import javax.swing.*;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/27
 * @time : 2:35 下午
 */
public class PersistJava {

    public static JavaSparkContext getsc(){
        SparkConf sparkConf = new SparkConf().setAppName("persistJava").setMaster("local");
        return new JavaSparkContext(sparkConf);
    }
    public static void main(String[] args) {
        JavaRDD lines = getsc().textFile("hdfs://bigdata-pro-m04:9000/user/caizhengjie/datas/2015082818").cache();

        // 第一次开始时间
        long begin = System.currentTimeMillis();
        System.out.println("行数:"+lines.count());
        // 第一次总时间
        System.out.println("第一次总时间:"+(System.currentTimeMillis() - begin));

        // 第二次开始时间
        long begin1 = System.currentTimeMillis();
        System.out.println("行数:"+lines.count());
        // 第二次总时间
        System.out.println("第二次总时间:"+(System.currentTimeMillis() - begin1));
    }
}
