package com.kfk.spark.sql

import com.kfk.spark.common.{Comm, CommSparkSessionScala}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/4
 * @time : 12:03 下午
 */
object OperationActionScala {
    def main(args: Array[String]): Unit = {
        val spark = CommSparkSessionScala.getSparkSession();

        val path = Comm.fileDirPath + "people.json"
        val df = spark.read.json(path)
        df.show()

        for (elem <- df.collect()) {
            System.out.println(elem)
        }

        System.out.println(df.first())

        for (elem <- df.take(2)) {
            System.out.println(elem)
        }

    }

}
