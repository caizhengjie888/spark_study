package com.kfk.spark.sql

import com.kfk.spark.common.{Comm, CommSparkSessionScala}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/8
 * @time : 4:46 下午
 */
object UDFScala {
    def main(args: Array[String]): Unit = {
        val spark = CommSparkSessionScala.getSparkSession()

        val userPath = Comm.fileDirPath + "users.json"
        spark.read.json(userPath).createOrReplaceTempView("user")

        // 自定义UDF将内容改为大写
        spark.udf.register("strUpper",(str:String) => str.toUpperCase)

        spark.sql("select deptName,strUpper(name) from user").show()

        /**
         * +--------+------------------+
         * |deptName|UDF:strUpper(name)|
         * +--------+------------------+
         * |  dept-1|           MICHAEL|
         * |  dept-2|              ANDY|
         * |  dept-1|              ALEX|
         * |  dept-2|            JUSTIN|
         * |  dept-2|            CHERRY|
         * |  dept-1|              JACK|
         * |  dept-2|              JONE|
         * |  dept-1|              LUCY|
         * |  dept-2|              LILI|
         * |  dept-2|              PONY|
         * +--------+------------------+
         */

    }
}
