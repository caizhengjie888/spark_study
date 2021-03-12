package com.kfk.spark.news_analysis_project;

import com.kfk.spark.common.CommStreamingContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/20
 * @time : 4:11 下午
 */
public class NewsRealTime {

    /**
     * input data:
     * 2020-12-20 1608451521565 364 422 fashion view
     * 2020-12-20 1608451521565 38682 708 aviation view
     * ...
     * @param args
     */
    public static void main(String[] args) throws InterruptedException {

        JavaStreamingContext jssc = CommStreamingContext.getJssc();

        // sparkstreaming与kafka连接
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "bigdata-pro-m04:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "streaming_kafka_1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // 设置topic
        Collection<String> topics = Collections.singletonList("spark");

        // kafka数据源
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        /**
         * stream -> map -> JavaDStream
         */
        JavaDStream<String> accessDstream = stream.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> v1) throws Exception {
                return v1.value();
            }
        });

        /**
         * accessDStream -> filter -> action(view)
         */
        JavaDStream<String> filterDstream = accessDstream.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {

                String[] lines = v1.split(" ");
                String action = lines[5];
                String actionValue = "view";
                if (actionValue.equals(action)){
                    return true;
                } else {
                    return false;
                }
            }
        });

        // 求网页的pv
        calculatePagePV(filterDstream);

        // 求网页的uv
        calculatePageUV(filterDstream);

        // 求注册用户数
        calculateRegistercount(accessDstream);

        // 求热门板块
        calculateUserSectionPV(accessDstream);

        jssc.start();
        jssc.awaitTermination();

    }

    /**
     * 求网页的pv
     * input data:
     * 2020-12-20 1608451521565 364 422 fashion view
     *
     * 数据演化过程：
     * filterDstream -> mapToPair -> <2020-12-20_422,1> -> reduceByKey -> <2020-12-20_422,5>
     *
     * @param filterDstream
     */
    public static void calculatePagePV(JavaDStream<String> filterDstream){

        /**
         * filterDstream -> mapToPair -> <2020-12-20_422,1>
         */
        JavaPairDStream<String,Integer> pairDstream = filterDstream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String,Integer> call(String lines) throws Exception {
                String[] line = lines.split(" ");
                return new Tuple2<String,Integer>(line[0] + "_" + line[3], 1);
            }
        });

        /**
         * pairDstream -> reduceByKey -> <2020-12-20_422,5>
         */
        JavaPairDStream<String,Integer> pvStream = pairDstream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        pvStream.print();

        /**
         * (2020-12-21_16,1)
         * (2020-12-21_548,1)
         * (2020-12-21_881,1)
         * (2020-12-21_27,1)
         * (2020-12-21_771,1)
         * (2020-12-21_344,2)
         * (2020-12-21_313,1)
         * (2020-12-21_89,1)
         * (2020-12-21_14,1)
         * (2020-12-21_366,1)
         * ...
         */
    }

    /**
     * 求网页的uv
     * input data:
     * 2020-12-20 1608451521565 364 422 fashion view
     * 2020-12-20 1608451521565 364 422 fashion view
     * 2020-12-20 1608451521565 365 422 fashion view
     * 2020-12-20 1608451521565 366 422 fashion view
     * 2020-12-20 1608451521565 367 422 fashion view
     * 2020-12-20 1608451521565 367 453 fashion view
     *
     * 数据演化过程：
     * 第一步：map
     * (2020-12-20,364,422)
     * (2020-12-20,364,422)
     * (2020-12-20,365,422)
     * (2020-12-20,366,422)
     * (2020-12-20,367,422)
     * (2020-12-20,367,453)
     *
     * 第二步：rdd -> distinct
     * (2020-12-20,364,422)
     * (2020-12-20,365,422)
     * (2020-12-20,366,422)
     * (2020-12-20,367,422)
     * (2020-12-20,367,453)
     *
     * 第三步：mapToPair
     * <2020-12-20_422,1>
     * <2020-12-20_422,1>
     * <2020-12-20_422,1>
     * <2020-12-20_422,1>
     * <2020-12-20_453,1>
     *
     * 第四步：reduceByKey
     * <2020-12-20_422,4>
     * <2020-12-20_453,1>
     *
     * @param filterDstream
     */
    public static void calculatePageUV(JavaDStream<String> filterDstream){

        /**
         * filterDstream -> map -> (2020-12-20,364,422)
         */
        JavaDStream<String> mapDstream = filterDstream.map(new Function<String, String>() {
            @Override
            public String call(String lines) throws Exception {
                String[] line = lines.split(" ");
                return line[0] + "," + line[2] + "," + line[3];
            }
        });

        /**
         * mapDstream -> distinct
         */
        JavaDStream<String> distinctDstream = mapDstream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> lines) throws Exception {
                return lines.distinct();
            }
        });

        /**
         * distinctDstream -> mapToPair -> <2020-12-20_422,1>
         */
        JavaPairDStream<String,Integer> pairDstream = distinctDstream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String lines) throws Exception {
                String[] line = lines.split(",");
                return new Tuple2<>(line[0] + "_" + line[2], 1);
            }
        });

        /**
         * pairDstream -> reduceByKey -> <2020-12-20_422,4>
         */
        JavaPairDStream<String,Integer> uvStream = pairDstream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        uvStream.print();

        /**
         * (2020-12-21_492,1)
         * (2020-12-21_85,2)
         * (2020-12-21_18,1)
         * (2020-12-21_27,2)
         * (2020-12-21_825,1)
         * (2020-12-21_366,1)
         * (2020-12-21_89,1)
         * (2020-12-21_14,2)
         * (2020-12-21_69,1)
         * (2020-12-21_188,1)
         * ...
         */

    }

    /**
     * 求注册用户数:过滤出action=register的数据就可以
     * input data:
     * 2020-12-20 1608451521565 364 422 fashion view
     *
     * 数据演化过程：
     * accessDStream -> filter -> action(register) -> mapToPair -> reduceByKey
     *
     * @param accessDstream
     */
    public static void calculateRegistercount(JavaDStream<String> accessDstream){

        /**
         * accessDStream -> filter -> action(register)
         */
        JavaDStream<String> filterDstream = accessDstream.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {

                String[] lines = v1.split(" ");
                String action = lines[5];
                String actionValue = "register";
                if (actionValue.equals(action)){
                    return true;
                } else {
                    return false;
                }
            }
        });


        /**
         * filterDstream -> mapToPair -> <2020-12-20_register,1>
         */
        JavaPairDStream<String,Integer> pairDstream = filterDstream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String,Integer> call(String lines) throws Exception {
                String[] line = lines.split(" ");
                return new Tuple2<String,Integer>(line[0] + "_" + line[5], 1);
            }
        });

        /**
         * pairDstream -> reduceByKey -> <2020-12-20_register,5>
         */
        JavaPairDStream<String,Integer> registerCountStream = pairDstream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        registerCountStream.print();

        /**
         * (2020-12-21_register,11)
         */
    }

    /**
     * 求出热门板块
     * input data:
     * 2020-12-20 1608451521565 364 422 fashion view
     *
     * 数据演化过程：
     * filterDstream -> mapToPair -> <2020-12-20_fashion,1> -> reduceByKey -> <2020-12-20_fashion,5>
     *
     * @param filterDstream
     */
    public static void calculateUserSectionPV(JavaDStream<String> filterDstream){

        /**
         * filterDstream -> mapToPair -> <2020-12-20_fashion,1>
         */
        JavaPairDStream<String,Integer> pairDstream = filterDstream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String,Integer> call(String lines) throws Exception {
                String[] line = lines.split(" ");
                return new Tuple2<String,Integer>(line[0] + "_" + line[4], 1);
            }
        });

        /**
         * pairDstream -> reduceByKey -> <2020-12-20_fashion,5>
         */
        JavaPairDStream<String,Integer> pvStream = pairDstream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        pvStream.print();

        /**
         * (2020-12-21_internet,16)
         * (2020-12-21_military,24)
         * (2020-12-21_aviation,21)
         * (2020-12-21_carton,19)
         * (2020-12-21_government,25)
         * (2020-12-21_tv-show,19)
         * (2020-12-21_country,14)
         * (2020-12-21_movie,13)
         * (2020-12-21_international,16)
         * ...
         */

    }
}
