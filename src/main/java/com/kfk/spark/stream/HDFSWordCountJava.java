package com.kfk.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/14
 * @time : 2:12 下午
 */
public class HDFSWordCountJava {
    public static void main(String[] args) throws InterruptedException {
        // Create a local StreamingContext with two working thread and batch interval of 5 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        String path = "hdfs://bigdata-pro-m04:9000/user/caizhengjie/datas/sparkstreaming/";

        // hdfs数据源
        JavaDStream<String> lines = jssc.textFileStream(path);

        // flatmap
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // map
        JavaPairDStream<String,Integer> pair =  words.mapToPair(word -> new Tuple2<>(word,1));

        // reduceByKey
        JavaPairDStream<String,Integer> wordcount = pair.reduceByKey((x,y) -> x+y);

        wordcount.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
