package com.kfk.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/24
 * @time : 10:33 下午
 */
public class ParallelizeJava {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6);
        JavaRDD<Integer> values = sc.parallelize(list);

        Object sum = values.reduce(new Function2<Integer,Integer,Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        System.out.println(sum);
    }
}
