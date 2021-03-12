package com.kfk.spark.common

import org.apache.spark.sql.SparkSession

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/2
 * @time : 10:02 下午
 */
object CommSparkSessionScala {
    def getSparkSession(): SparkSession ={
        val spark = SparkSession
                .builder
                .appName("CommSparkSessionScala")
                .master("local")
                .config("spark.sql.warehouse.dir", "/Users/caizhengjie/Document/spark/spark-warehouse")
                .getOrCreate

        return spark
    }
}
