package com.kfk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/14
 * @time : 8:11 下午
 */
object HDFSWordCountScala {
    def main(args: Array[String]): Unit = {
        // Create a local StreamingContext with two working thread and batch interval of 5 second
        val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
        val jssc = new StreamingContext(conf, Durations.seconds(1))

        val path = "hdfs://bigdata-pro-m04:9000/user/caizhengjie/datas/sparkstreaming/"
        val lines = jssc.textFileStream(path)

        // flatmap
        val words = lines.flatMap(word => word.split(" "))

        // map
        val pair = words.map(x => (x,1))

        // reduceByKey
        val wordcount = pair.reduceByKey((x,y) => x+y)

        wordcount.print()


        jssc.start()
        jssc.awaitTermination()
    }

}
