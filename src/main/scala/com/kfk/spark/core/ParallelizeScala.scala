package com.kfk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/24
 * @time : 10:43 下午
 */
object ParallelizeScala {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("parallelize").setMaster("local")
        val sc = new SparkContext(sparkConf)

        val list = Array(1,2,3,4,5,6)

        val values = sc.parallelize(list)

        val num = values.reduce(_ +_)
        println(num)

    }
}
