package com.kfk.spark.common

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}


/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/14
 * @time : 9:55 下午
 */
object CommStreamingContextScala {

    def getJssc: StreamingContext = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("CommStreamingContext")
        new StreamingContext(conf, Durations.seconds(2))
    }

}
