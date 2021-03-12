package com.kfk.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/28
 * @time : 11:28 上午
 */
public class SecondSortJava {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("SecondSortJava").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /**
         * class1 90
         * class2 93
         * class1 97        ->  class1 (89,90,97...) class2(89,93...) class3(86,99...)
         * class1 89
         * class3 99
         * ...
         */
        List list = Arrays.asList("class1 90","class2 93","class1 97",
                "class1 89","class3 99","class3 78",
                "class1 79","class2 85","class2 89",
                "class2 96","class3 92","class3 86");

        JavaRDD rdd = sc.parallelize(list);

        JavaPairRDD<SecondSortKey,String> beginsortValues = rdd.mapToPair(new PairFunction<String, SecondSortKey, String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(String line) throws Exception {
                String first = line.split(" ")[0];
                int second = Integer.parseInt(line.split(" ")[1]);
                SecondSortKey secondSortKey = new SecondSortKey(first,second);

                return new Tuple2<SecondSortKey,String>(secondSortKey,line);
            }
        });

        JavaPairRDD<SecondSortKey,String> sortValues = beginsortValues.sortByKey(false);

        sortValues.foreach(new VoidFunction<Tuple2<SecondSortKey, String>>() {
            @Override
            public void call(Tuple2<SecondSortKey, String> secondSortKeyStringTuple2) throws Exception {
                System.out.println(secondSortKeyStringTuple2._2);
            }
        });
    }
}
