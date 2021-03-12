package com.kfk.spark.window_hotwords_project

import com.kfk.spark.common.CommStreamingContextScala
import org.apache.spark.streaming.Seconds


/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/18
 * @time : 4:47 下午
 */
object WindowHotWordScala {
    def main(args: Array[String]): Unit = {
        val jssc = CommStreamingContextScala.getJssc

        val inputDstream = jssc.socketTextStream("bigdata-pro-m04", 9999)

        /**
         * 数据模型：java
         * hive
         * spark
         * java
         */
        val pairDStream = inputDstream.map(x => (x,1))

        /**
         * <java,1>
         * <hive,1>
         * ...
         */
        val windowWordCount = pairDStream.reduceByKeyAndWindow((x:Int,y:Int) => x+y,Seconds(60),Seconds(10))

        /**
         * 热点搜索词滑动统计，每隔10秒钟，统计最近60秒钟的搜索词的搜索频次，
         * 并打印出 排名最靠前的3个搜索词以及出现次数
         */
        val finalDStream = windowWordCount.transform(x => {

            val sortRDD = x.map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1))

            val list = sortRDD.take(3)

            jssc.sparkContext.parallelize(list)
        })

        finalDStream.print()

        jssc.start()
        jssc.awaitTermination()

    }

}
