package com.kfk.spark.core;

import com.kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/29
 * @time : 8:52 下午
 */
public class CartesionJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        List list1 = Arrays.asList("衣服-1","衣服-2");
        List list2 = Arrays.asList("裤子-1","裤子-2");

        JavaRDD<String> rdd1 = sc.parallelize(list1);
        JavaRDD<String> rdd2 = sc.parallelize(list2);

        /**
         * (cherry,alex)(cherry,jack)(herry,alex)(herry,jack)
         */
        JavaPairRDD<String,String> carteValues = rdd1.cartesian(rdd2);

        for (Object obj : carteValues.collect()){
            System.out.println(obj);

        }

    }
}
