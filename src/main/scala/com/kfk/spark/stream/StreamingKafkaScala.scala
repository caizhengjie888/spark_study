package com.kfk.spark.stream

import com.kfk.spark.common.CommStreamingContextScala
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/14
 * @time : 9:56 下午
 */
object StreamingKafkaScala {
    def main(args: Array[String]): Unit = {
        val jssc = CommStreamingContextScala.getJssc;

        // sparkstreaming与kafka连接
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "bigdata-pro-m04:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "streaming_kafka_1",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        // 设置topic
        val topics = Array("spark")

        // kafka数据源
        val stream = KafkaUtils.createDirectStream[String, String](
            jssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        // flatmap
        val words = stream.flatMap(record => record.value().trim.split(" "))

        // map
        val pair = words.map(x => (x,1))

        // reduceByKey
        val wordcount = pair.reduceByKey((x,y) => x+y)

        wordcount.print()

        jssc.start()
        jssc.awaitTermination()
    }

}
