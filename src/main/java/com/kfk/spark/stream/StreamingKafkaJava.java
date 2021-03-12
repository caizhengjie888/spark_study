package com.kfk.spark.stream;

import com.kfk.spark.common.CommStreamingContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/14
 * @time : 8:20 下午
 */
public class StreamingKafkaJava {
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
        Collection<String> topics = Arrays.asList("spark");

        // kafka数据源
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        // flatmap
        JavaDStream<String> words = stream.flatMap(record -> Arrays.asList(record.value().trim().split(" ")).iterator());

        // map
        JavaPairDStream<String,Integer> pair = words.mapToPair(word -> new Tuple2<>(word,1));

        // reduceByKey
        JavaPairDStream<String,Integer> wordcount = pair.reduceByKey((x,y) -> x+y);

        wordcount.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
