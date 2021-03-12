package com.kfk.spark.window_hotwords_project;

import com.kfk.spark.common.CommStreamingContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;


/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/18
 * @time : 2:03 下午
 */
public class WindowHotWordJava {
    static JavaStreamingContext jssc = null;

    public static void main(String[] args) throws InterruptedException {
        jssc = CommStreamingContext.getJssc();

        /**
         * 数据模型：java
         *         hive
         *         spark
         *         java
         */

        JavaReceiverInputDStream<String> inputDstream = jssc.socketTextStream("bigdata-pro-m04",9999);

        /**
         * <java,1>
         * <hive,1>
         * ...
         */
        JavaPairDStream<String, Integer> pair = inputDstream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                return new Tuple2<>(line,1);
            }
        });

        /**
         * <java,5>
         * <hive,3>
         * <spark,6>
         * <flink,10>
         */
        JavaPairDStream<String, Integer> windowWordCount = pair.reduceByKeyAndWindow(new Function2<Integer,Integer,Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }, Durations.seconds(60),Durations.seconds(10));

        JavaDStream<Tuple2<String, Integer>> finalStream = windowWordCount.transform(
                new Function<JavaPairRDD<String, Integer>, JavaRDD<Tuple2<String, Integer>>>() {
            @Override
            public JavaRDD<Tuple2<String, Integer>> call(JavaPairRDD<String, Integer> line) throws Exception {

                JavaPairRDD<Integer,String> beginPair = line.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return new Tuple2<>(stringIntegerTuple2._2,stringIntegerTuple2._1);
                    }
                });

                JavaPairRDD<Integer,String> sortRdd = beginPair.sortByKey(false);

                JavaPairRDD<String,Integer> sortPair = sortRdd.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String,Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                        return new Tuple2<>(integerStringTuple2._2,integerStringTuple2._1);
                    }
                });

                List<Tuple2<String,Integer>> wordList = sortPair.take(3);


                for (Tuple2<String, Integer> stringIntegerTuple2 : wordList) {
                    System.out.println(stringIntegerTuple2._1 + " : " + stringIntegerTuple2._2);
                }

                return jssc.sparkContext().parallelize(wordList);

            }
        });

        finalStream.print();

        jssc.start();

        jssc.awaitTermination();

    }
}
