package com.kfk.spark.core;

import com.kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/29
 * @time : 7:31 下午
 */
public class MapPartitionWithIndexJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        List list = Arrays.asList("alex","herry","lili","ben","jack","jone","cherry");

        // 将rdd分为3个partition
        JavaRDD rdd = sc.parallelize(list,3);

        // 查看每个数据对应的分区号
        JavaRDD<String> indexValues = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while (iterator.hasNext()){
                    String indexStr = iterator.next() + " : " + (index+1);
                    list.add(indexStr);
                }
                return list.iterator();
            }
        },false);

        indexValues.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
