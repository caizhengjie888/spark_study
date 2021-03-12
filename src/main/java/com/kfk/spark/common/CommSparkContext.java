package com.kfk.spark.common;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/28
 * @time : 6:18 下午
 */
public class CommSparkContext {

    public static JavaSparkContext getsc(){
        SparkConf sparkConf = new SparkConf().setAppName("CommSparkContext").setMaster("local");
        return new JavaSparkContext(sparkConf);
    }
}
