package com.kfk.spark.core;

import com.kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.*;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/29
 * @time : 4:18 下午
 */
public class MapPartitionJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        List list = Arrays.asList("alex","herry","lili","ben","jack","jone","cherry");

        // 将rdd分为2个partition
        JavaRDD rdd = sc.parallelize(list,2);

        final Map<String,Double> map = new HashMap<String,Double>();
        map.put("alex",98.6);
        map.put("herry",89.5);
        map.put("lili",87.3);
        map.put("ben",91.2);
        map.put("jack",78.9);
        map.put("jone",95.4);
        map.put("cherry",96.1);

        // 对每一个分区中的数据同时做处理
        JavaRDD<Double>  mapPartition = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, Double>() {
            @Override
            public Iterator<Double> call(Iterator iterator) throws Exception {
                List<Double> list = new ArrayList<Double>();
                while (iterator.hasNext()){
                    String userName = String.valueOf(iterator.next());
                    Double score = map.get(userName);
                    list.add(score);
                }
                return list.iterator();
            }
        });

        mapPartition.foreach(new VoidFunction<Double>() {
            @Override
            public void call(Double o) throws Exception {
                System.out.println(o);
            }
        });
    }
}
