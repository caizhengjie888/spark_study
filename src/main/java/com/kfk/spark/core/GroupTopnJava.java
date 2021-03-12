package com.kfk.spark.core;

import com.kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/28
 * @time : 8:38 下午
 */
public class GroupTopnJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        List<String> list = Arrays.asList("class1 90",
                "class2 93",
                "class1 97",
                "class1 89",
                "class3 99",
                "class1 34",
                "class1 45",
                "class1 99",
                "class3 78",
                "class1 79",
                "class2 85",
                "class2 89",
                "class2 96",
                "class3 92",
                "class1 98",
                "class3 86");

        JavaRDD<String> rdd = sc.parallelize(list);

        /**
         *  class1 90       mapToPair() -> (class1,90)
         *  class2 93
         *  class1 97
         *  class1 89
         *  ...
         */
        JavaPairRDD<String,Integer> beginGroupValue = rdd.mapToPair(new PairFunction<String,String,Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String key = line.split(" ")[0];
                Integer value = Integer.parseInt(line.split(" ")[1]);
                return new Tuple2<String, Integer>(key,value);
            }
        });


        /**
         * <class1,(90,97,98,89,79,34,45,99)>
         * ...
         */
        JavaPairRDD<String,Iterable<Integer>> groupValues = beginGroupValue.groupByKey();

        /**
         * <class1,(90,97,98,89,79,34,45,99)>      -> <class1,(99,98,97,90,89,79,45,34)>
         */
        JavaPairRDD<String,Iterable<Integer>> groupTopValues = groupValues.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> groupValue) throws Exception {
                Iterator<Integer> iterator = groupValue._2.iterator();

                /**
                 * top(n) 算法演化过程
                 * top(3)
                 * null -> 90
                 * 97   -> 97,90 (add)
                 * 98   -> 98,97,90 (add)
                 * 89   -> 98,97,90
                 * 79   -> 98,97,90
                 * 34   -> 98,97,90
                 * 45   -> 98,97,90
                 * 99   -> 99,98,97 (remove -> 90)
                 *
                 * 97   -> 97,90 (add)
                 * 89   -> 97,90,89
                 */

                // 90,97,89,79,34,45,99
                // [90,]
                LinkedList<Integer> linkedList = new LinkedList<Integer>();
                while (iterator.hasNext()){
                    Integer value = (Integer) iterator.next();
                    if (linkedList.size() == 0){
                        linkedList.add(value);
                    } else {
                        for (int i = 0; i < linkedList.size(); i++){
                            if (value > linkedList.get(i)){
                                linkedList.add(i,value);
                                if (linkedList.size() > 3){
                                    linkedList.removeLast();
                                }
                                break;
                            } else {
                                if (linkedList.size() < 3){
                                    linkedList.add(value);
                                    break;
                                }
                            }
                        }
                    }
                }
                return new Tuple2<String, Iterable<Integer>>(groupValue._1,linkedList);
            }
        });

        groupTopValues.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2._1);

                Iterator<Integer> iterator = stringIterableTuple2._2.iterator();
                while (iterator.hasNext()){
                    System.out.println(iterator.next());
                }
            }
        });
    }
}
