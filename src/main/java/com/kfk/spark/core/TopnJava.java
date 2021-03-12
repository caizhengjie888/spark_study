package com.kfk.spark.core;

import com.kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/28
 * @time : 6:26 下午
 */
public class TopnJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        List list = Arrays.asList(34,54,32,12,67,25,84,58,39,18,39,81);

        /**
         * 数据模型
         * 34,54,32,12...
         * map()        -> (34,34)(54,54)(32,32)(12,12)
         * sortByKey()  -> (54,54)(34,34)(32,32)(12,12)
         * map()        -> 54,34,32,12
         * take(2)      -> 54,34
         * for()
         */
        JavaRDD rdd = sc.parallelize(list);
        JavaPairRDD<Integer,Integer> beginSortValue = rdd.mapToPair(new PairFunction<Integer,Integer,Integer>() {
            @Override
            public Tuple2<Integer,Integer> call(Integer value) throws Exception {
                return new Tuple2<Integer, Integer>(value,value);
            }
        });

        /**
         * sortByKey()  -> (54,54)(34,34)(32,32)(12,12)
         */
        JavaPairRDD<Integer,Integer> sortKey = beginSortValue.sortByKey(false);

        /**
         * map()        -> 54,34,32,12
         */
        JavaRDD<Integer> sortMapKey = sortKey.map(new Function<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                return integerIntegerTuple2._2;
            }
        });

        /**
         * take(2)      -> 54,34
         */
        List<Integer> sortKeyList = sortMapKey.take(3);
        for (Integer value : sortKeyList){
            System.out.println(value);
        }

    }
}
