package com.kfk.spark.sql

import com.kfk.spark.common.{Comm, CommSparkSessionScala}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/8
 * @time : 8:06 下午
 */
object UDAFScala {
    def main(args: Array[String]): Unit = {
        val spark = CommSparkSessionScala.getSparkSession()

        val userPath = Comm.fileDirPath + "users.json"
        spark.read.json(userPath).createOrReplaceTempView("user")

        // 自定义UDAF，聚合函数
        spark.udf.register("count",new MyCount)

        spark.sql("select deptName,count(deptName) count from user group by deptName").show()

        /**
         * +--------+-----+
         * |deptName|count|
         * +--------+-----+
         * |  dept-1|    4|
         * |  dept-2|    6|
         * +--------+-----+
         */
    }
}
