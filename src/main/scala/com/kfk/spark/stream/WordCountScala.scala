package com.kfk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/14
 * @time : 12:54 下午
 */
object WordCountScala {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
        val ssc = new StreamingContext(conf, Seconds(5))

        val lines = ssc.socketTextStream("bigdata-pro-m04", 9999)

        // flatmap
        val words = lines.flatMap(word => word.split(" "))

        // map
        val pair = words.map(x => (x,1))

        // reduceByKey
        val wordcount = pair.reduceByKey((x,y) => x+y)

        wordcount.print()

        ssc.start()
        ssc.awaitTermination()

    }
}
