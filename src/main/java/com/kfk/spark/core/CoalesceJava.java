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
 * @time : 9:40 下午
 */
public class CoalesceJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        List list = Arrays.asList("alex","herry","lili","ben","jack","jone","cherry","lucy","pony","leo");
        JavaRDD rdd = sc.parallelize(list,4);

        // 查看每个值对应每个分区
        JavaRDD<String> indexValues1 = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator iterator) throws Exception {

                List<String> list = new ArrayList<String>();
                while (iterator.hasNext()){
                    String indexStr = iterator.next() + " " + "以前分区" + " : " + (index+1);
                    list.add(indexStr);
                }
                return list.iterator();
            }
        },false);

        // 合并为两个分区
        JavaRDD<String>  coalesceValues = indexValues1.coalesce(2);

        // 合并分区之后重新查看每个值对应每个分区
        JavaRDD<String> indexValues2 = coalesceValues.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator iterator) throws Exception {

                List<String> list = new ArrayList<String>();
                while (iterator.hasNext()){
                    String indexStr = iterator.next() + " " + "现在分区" + " : " + (index+1);
                    list.add(indexStr);
                }
                return list.iterator();
            }
        },false);


        indexValues2.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
