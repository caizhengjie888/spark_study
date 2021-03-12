package com.kfk.spark.sql

import com.kfk.spark.common.{CommScala, CommSparkSessionScala}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/3
 * @time : 10:05 下午
 */
object RDDToDFProgramScala {
    def main(args: Array[String]): Unit = {

        val spark = CommSparkSessionScala.getSparkSession();

        /**
         * 数据源
         * Michael, 29
         * Andy, 30
         * Justin, 19
         */
        val path = CommScala.fileDirPath + "people.txt";

        // 构造Schema
        val scheme = StructType(Array(
            StructField("name",DataTypes.StringType,true),
            StructField("age",DataTypes.LongType,true)
        ))

        // 将rdd(people)转换成RDD[Row]
        val rdd = spark.sparkContext.textFile(path).map(line => {
            line.split(",")
        }).map(x => {
            Row(x(0),x(1).trim.toLong)
        })

        // 通过rdd和scheme创建DataFrame
        val personDataFrame = spark.createDataFrame(rdd,scheme)

        personDataFrame.show()

        /**
         * +-------+---+
         * |   name|age|
         * +-------+---+
         * |Michael| 29|
         * |   Andy| 30|
         * | Justin| 19|
         * +-------+---+
         */

        // 创建临时表
        personDataFrame.createOrReplaceTempView("person")

        val resultDataFrame = spark.sql("select * from person a where a.age > 20")
        resultDataFrame.show()

        /**
         * +-------+---+
         * |   name|age|
         * +-------+---+
         * |Michael| 29|
         * |   Andy| 30|
         * +-------+---+
         */

        for (elem <- resultDataFrame.collect()) {
            System.out.println(elem)
        }

        /**
         * [Michael,29]
         * [Andy,30]
         */

    }
}
