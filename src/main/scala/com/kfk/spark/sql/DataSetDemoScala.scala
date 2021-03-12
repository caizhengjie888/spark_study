package com.kfk.spark.sql

import com.kfk.spark.common.{CommScala, CommSparkSessionScala}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/5
 * @time : 4:10 下午
 */
object DataSetDemoScala {
    case class Person(name: String, age: Int)
    def main(args: Array[String]): Unit = {

        val spark = CommSparkSessionScala.getSparkSession()

        /**
         * 数据源
         * Michael, 29
         * Andy, 30
         * Justin, 19
         */
        val path = CommScala.fileDirPath + "people.txt";

        //使用toDS()函数需要导入隐式转换
        import spark.implicits._

        // 方案一：通过toDS()将RDD转换成DataSet
        val dataDS = spark.sparkContext.textFile(path).map(line => line.split(",")).map(x => {
            Person(x(0),x(1).trim.toInt)
        }).toDS()

        dataDS.show()

        /**
         * +-------+---+
         * |   name|age|
         * +-------+---+
         * |Michael| 29|
         * |   Andy| 30|
         * | Justin| 19|
         * +-------+---+
         */

        // 方案二：使用spark.createDataset(rdd)将RDD转换成DataSet
        val rdd = spark.sparkContext.textFile(path).map(line => {
            line.split(",")
        }).map(x => {
            Person(x(0),x(1).trim.toInt)
        })

        val dataDS1 = spark.createDataset(rdd)
        dataDS1.show()

        /**
         * +-------+---+
         * |   name|age|
         * +-------+---+
         * |Michael| 29|
         * |   Andy| 30|
         * | Justin| 19|
         * +-------+---+
         */

        val rdd1 = dataDS1.rdd

        for (elem <- rdd1.collect()) {
            System.out.println(elem.name + " : " + elem.age)
        }

        /**
         * Michael : 29
         * Andy : 30
         * Justin : 19
         */
    }
}
