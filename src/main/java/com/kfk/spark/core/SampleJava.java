package com.kfk.spark.core;

import com.kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/30
 * @time : 9:37 上午
 */
public class SampleJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        List list = Arrays.asList("alex","herry","lili","ben","jack","jone","cherry","lucy","pony","leo");

        JavaRDD rdd = sc.parallelize(list);

        // 随机抽取50%
        JavaRDD<String> sampleValues = rdd.sample(false,0.5);

        for (Object obj : sampleValues.collect()){
            System.out.println(obj);
        }
    }
}
