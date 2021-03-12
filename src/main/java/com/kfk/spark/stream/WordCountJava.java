package com.kfk.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


/**
 * lambda表达式写法
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/13
 * @time : 9:25 下午
 */
public class WordCountJava {
    public static void main(String[] args) throws InterruptedException {

        // Create a local StreamingContext with two working thread and batch interval of 5 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("bigdata-pro-m04",9999);

//        // flatmap
//        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//
//        // map
//        JavaPairDStream<String,Integer> pair =  words.mapToPair(word -> new Tuple2<>(word,1));
//
//        // reduceByKey
//        JavaPairDStream<String,Integer> wordcount = pair.reduceByKey((x,y) -> x+y);
//
//        wordcount.print();

        JavaDStream<String> mapDstream = lines.map(new Function<String, String>() {
            @Override
            public String call(String lines) throws Exception {
                String[] line = lines.split(" ");
                return line[0] + "," + line[2] + "," + line[3];
            }
        });

        JavaPairDStream<String, String> pairDstream = mapDstream.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String lines) throws Exception {
                String[] line = lines.split(",");
                return new Tuple2<>(line[0] + "_" + line[2], line[1]);
            }
        });

        JavaPairDStream<Tuple2<String, String>, Long> countByValueStream = pairDstream.countByValue();

//        JavaPairDStream<Tuple2<String, String>, Long> filterDstream = count.filter(new Function<Tuple2<Tuple2<String, String>, Long>, Boolean>() {
//            @Override
//            public Boolean call(Tuple2<Tuple2<String, String>, Long> v1) throws Exception {
//
//                if (v1._2 == 1){
//                    return true;
//                } else {
//                    return false;
//                }
//            }
//        });

        JavaPairDStream<String, Long> mapToPairStream = countByValueStream.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Long>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<Tuple2<String, String>, Long> tuple2LongTuple2) throws Exception {
                return new Tuple2<String, Long>(tuple2LongTuple2._1._1,tuple2LongTuple2._2);
            }
        });

//        mapToPairStream.groupByKey().filter(new Function<Tuple2<String, Iterable<Long>>, Boolean>() {
//            @Override
//            public Boolean call(Tuple2<String, Iterable<Long>> v1) throws Exception {
//
//                Iterator<Long> iterator = v1._2.iterator();
//                while (iterator.hasNext()){
//                    if (iterator.next() == 1){
//                        return true;
//                    } else {
//                        return false;
//                    }
//                }
//            }
//        });



        mapToPairStream.print();
        jssc.start();
        jssc.awaitTermination();

    }
}
