package com.kfk.spark.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/27
 * @time : 7:52 下午
 */
public class VarJava {

    public static JavaSparkContext getsc(){
        SparkConf sparkConf = new SparkConf().setAppName("VarJava").setMaster("local");
        return new JavaSparkContext(sparkConf);
    }
    public static void main(String[] args) {

        List list = Arrays.asList(1,2,3,4,5);

        JavaSparkContext sc = getsc();

        // 广播变量
        final Broadcast broadcast = sc.broadcast(10);
        JavaRDD<Integer> javaRdd = sc.parallelize(list);

        JavaRDD mapValues = javaRdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer value) throws Exception {
                return  value * (Integer) broadcast.getValue();
            }
        });

        mapValues.foreach(new VoidFunction() {
            @Override
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });

        // 累加变量
        final Accumulator accumulator = sc.accumulator(1);
        javaRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                accumulator.add(integer);
            }
        });

        System.out.println(accumulator.value());
    }
}
