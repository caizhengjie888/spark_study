package com.kfk.spark.core;

import com.kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/29
 * @time : 10:22 下午
 */
public class TakeSampleJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        List list = Arrays.asList("alex","herry","lili","ben","jack","jone","cherry","lucy","pony","leo");
        JavaRDD rdd = sc.parallelize(list,4);

        // 随机抽取3个数据
        List takeSampleList = rdd.takeSample(false,3);

        for (Object obj : takeSampleList){
            System.out.println(obj);
        }
    }
}
