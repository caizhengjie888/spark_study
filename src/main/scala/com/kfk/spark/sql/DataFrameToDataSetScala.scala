package com.kfk.spark.sql

import com.kfk.spark.common.{Comm, CommScala, CommSparkSessionScala}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/5
 * @time : 5:35 下午
 */
object DataFrameToDataSetScala {
    case class Person(name:String,age:Long)
    def main(args: Array[String]): Unit = {

        val spark = CommSparkSessionScala.getSparkSession()
        import spark.implicits._
        /**
         * 数据源
         * Michael, 29
         * Andy, 30
         * Justin, 19
         */
        val path = CommScala.fileDirPath + "people.txt";

        val df = spark.sparkContext.textFile(path).map(line => line.split(",")).map(x => Person(x(0),x(1).trim.toLong)).toDF()

        df.show()

        // 将DF转换成DS
        val personDS = df.as[Person]
        personDS.show()
        personDS.printSchema()

        // 将DS装换成DF
        val personDF = personDS.toDF()
        personDF.show()
        personDF.printSchema()

        /**
         * +-------+---+
         * |   name|age|
         * +-------+---+
         * |Michael| 29|
         * |   Andy| 30|
         * | Justin| 19|
         * +-------+---+
         *
         * root
         *  |-- name: string (nullable = true)
         *  |-- age: long (nullable = false)
        */
    }
}
