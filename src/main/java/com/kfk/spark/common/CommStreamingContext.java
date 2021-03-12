package com.kfk.spark.common;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/14
 * @time : 8:23 下午
 */
public class CommStreamingContext {

    public static JavaStreamingContext getJssc(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("CommStreamingContext");
        return new JavaStreamingContext(conf, Durations.seconds(5));
    }
}
