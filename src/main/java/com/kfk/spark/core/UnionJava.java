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
 * @time : 9:51 上午
 */
public class UnionJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();
        List list1 = Arrays.asList("alex","herry","lili","ben","jack");
        List list2 = Arrays.asList("jone","cherry","lucy","pony","leo");

        JavaRDD rdd1 = sc.parallelize(list1);
        JavaRDD rdd2 = sc.parallelize(list2);

        // 将两个RDD的数据合并为一个RDD
        JavaRDD<String> unionValues = rdd1.union(rdd2);

        for (Object obj : unionValues.collect()){
            System.out.println(obj);
        }
    }
}
