package com.kfk.spark.sql

import com.kfk.spark.common.{Comm, CommSparkSessionScala}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/4
 * @time : 12:48 下午
 * 持久化 ： cache  persist
 * 创建临时视图 ： createTempView  createOrReplaceTempView
 * 获取执行计划 ： explain
 * 查看schema ： printSchema
 * 写数据到外部的数据存储系统 ： write
 * ds与df之间的转换 ： as toDF
 */
object BasicOperationScala {

    case class Person(name:String,age:Long)
    def main(args: Array[String]): Unit = {
        val spark = CommSparkSessionScala.getSparkSession()

        val path = Comm.fileDirPath + "people.json"
        val df = spark.read.json(path)
        df.show()

        // 持久化
        df.cache()

        // 创建临时视图
        df.createOrReplaceTempView("person")
        val resultDF = spark.sql("select * from person")

        // 获取执行计划
        resultDF.explain()

        // 查看schema
        df.printSchema()

        // save
        df.select("name").write.save("path")

        import spark.implicits._
        // 将DF转换成DS
        val personDS = df.as[Person]
        personDS.show()
        personDS.printSchema()

        // 将DS装换成DF
        val personDF = personDS.toDF()
        personDF.show()
        personDF.printSchema()

    }
}
