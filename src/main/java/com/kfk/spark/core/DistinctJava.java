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
 * @time : 10:03 上午
 */
public class DistinctJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        List list = Arrays.asList("alex","herry","lili","ben","jack","alex","herry");

        JavaRDD rdd = sc.parallelize(list);

        // 对RDD中的数据进行去重
        JavaRDD<String> distinctValues = rdd.distinct();

        for (Object obj : distinctValues.collect()){
            System.out.println(obj);
        }
    }
}
