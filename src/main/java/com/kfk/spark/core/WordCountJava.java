package com.kfk.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/22
 * @time : 10:19 下午
 */
public class WordCountJava {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("wordCountApp").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("hdfs://bigdata-pro-m04:9000/user/caizhengjie/datas/wordcount.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String,String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD<String,Integer> word = words.mapToPair(new PairFunction<String,String,Integer>() {
            @Override
            public Tuple2<String,Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        JavaPairRDD<String, Integer> wordcount = word.reduceByKey(new Function2<Integer,Integer,Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+ v2;
            }
        });

        wordcount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            @Override
            public void call(Tuple2<String,Integer> o) throws Exception {
                System.out.println(o._1 + " : " + o._2);
            }
        });

    }
}
