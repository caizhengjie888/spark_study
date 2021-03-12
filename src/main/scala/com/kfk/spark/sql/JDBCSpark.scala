package com.kfk.spark.sql

import com.kfk.spark.common.CommSparkSessionScala
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/11
 * @time : 2:11 下午
 */
object JDBCSpark {
    def main(args: Array[String]): Unit = {
        val spark = CommSparkSessionScala.getSparkSession()

        // 方法一创建jdbc链接
        val jdbcDF = spark.read
                .format("jdbc")
                .option("url", "jdbc:mysql://bigdata-pro-m04/spark")
                .option("dbtable", "person")
                .option("user", "root")
                .option("password", "199911")
                .load()

        jdbcDF.show()

        getData(spark)
        writeData(jdbcDF)

    }

    /**
     * 读取mysql中的数据
     * @param spark
     */
    def getData(spark : SparkSession): Unit ={

        // 方法二创建jdbc链接
        val connectionProperties = new Properties()
        connectionProperties.put("user", "root")
        connectionProperties.put("password", "199911")
        val jdbcDF2 = spark.read.jdbc("jdbc:mysql://bigdata-pro-m04/spark", "person", connectionProperties)

        jdbcDF2.show()

    }

    /**
     * 将数据写入到mysql中
     * @param jdbcDF
     */
    def writeData(jdbcDF : DataFrame): Unit ={
        jdbcDF.write
                .format("jdbc")
                .option("url", "jdbc:mysql://bigdata-pro-m04/spark")
                .option("dbtable", "person_info")
                .option("user", "root")
                .option("password", "199911")
                .save()
    }
}
