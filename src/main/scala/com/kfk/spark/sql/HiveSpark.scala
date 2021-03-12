package com.kfk.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/9
 * @time : 4:01 下午
 */
object HiveSpark {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder
                .appName("Spark Hive Example")
                .master("local")
                .config("spark.sql.warehouse.dir", "/Users/caizhengjie/Document/spark/spark-warehouse")
                .enableHiveSupport()
                .getOrCreate

        spark.sql("select * from hivespark.person").show()
    }

}
