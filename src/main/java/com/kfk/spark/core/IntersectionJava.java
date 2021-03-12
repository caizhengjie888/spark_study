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
 * @time : 9:59 上午
 */
public class IntersectionJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();
        List list1 = Arrays.asList("alex","herry","lili","ben","jack");
        List list2 = Arrays.asList("jone","alex","lili","pony","leo");

        JavaRDD rdd1 = sc.parallelize(list1);
        JavaRDD rdd2 = sc.parallelize(list2);

        // 获取两个RDD相同的数据
        JavaRDD<String> intersectionValues = rdd1.intersection(rdd2);
        for (Object obj : intersectionValues.collect()){
            System.out.println(obj);
        }
    }
}
