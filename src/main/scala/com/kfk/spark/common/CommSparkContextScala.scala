package com.kfk.spark.common

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/28
 * @time : 6:18 下午
 */
object CommSparkContextScala {

    def getsc():SparkContext ={

        val sparkConf = new SparkConf().setAppName("CommSparkContextScala").setMaster("local")
        return new SparkContext(sparkConf)
    }
}
