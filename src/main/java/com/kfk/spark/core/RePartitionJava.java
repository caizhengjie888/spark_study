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
 * @time : 10:12 下午
 */
public class RePartitionJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        List list = Arrays.asList("alex","herry","lili","ben","jack","jone","cherry","lucy","pony","leo");
        JavaRDD rdd = sc.parallelize(list,2);

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

        // 增加为四个分区
        JavaRDD<String>  repartitionValues = indexValues1.repartition(4);

        // 增加四个分区之后查看每个值对应每个分区
        JavaRDD<String> indexValues2 = repartitionValues.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
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
