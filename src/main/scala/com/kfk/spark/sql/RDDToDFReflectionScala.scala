package com.kfk.spark.sql

import com.kfk.spark.common.{CommScala, CommSparkSessionScala}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/3
 * @time : 6:32 下午
 */
object RDDToDFReflectionScala {
    case class Person(name : String, age : Long)
    def main(args: Array[String]): Unit = {

        val spark = CommSparkSessionScala.getSparkSession();

        /**
         * 数据源
         * Michael, 29
         * Andy, 30
         * Justin, 19
         */
        val path = CommScala.fileDirPath + "people.txt";

        // 通过case class反射将rdd转换成DataFrame
        import spark.implicits._
        val personDf = spark.sparkContext.textFile(path).map(line => line.split(",")).map(x => {
            Person(x(0),x(1).trim.toLong)
        }).toDF()

        personDf.show()

        // 直接将字段名称传入toDF中转换成DataFrame
        val personDf1 = spark.sparkContext.textFile(path).map(line => line.split(",")).map(x => {
            (x(0),x(1).trim)
        }).toDF("name", "age")

        personDf1.show()



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
        personDf.createOrReplaceTempView("person")
        val resultDf = spark.sql("select * from person a where a.age > 20")
        resultDf.show()

        /**
         * +-------+---+
         * |   name|age|
         * +-------+---+
         * |Michael| 29|
         * |   Andy| 30|
         * +-------+---+
         */

        // 将DataFrame转换成rdd
        val resultRdd = resultDf.rdd.map(row => {
            val name = row.getAs[String]("name")
            val age = row.getAs[Long]("age")

            Person(name,age)
        })

        for (elem <- resultRdd.collect()) {
            System.out.println(elem.name + " : " + elem.age)
        }

        /**
         * Michael : 29
         * Andy : 30
         */
    }
}
