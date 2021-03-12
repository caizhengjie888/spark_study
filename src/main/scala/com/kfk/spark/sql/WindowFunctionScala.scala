package com.kfk.spark.sql

import com.kfk.spark.common.{Comm, CommSparkSessionScala}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/8
 * @time : 12:22 下午
 */
object WindowFunctionScala {
    def main(args: Array[String]): Unit = {
        val spark = CommSparkSessionScala.getSparkSession()

        val userPath = Comm.fileDirPath + "users.json"
        spark.read.json(userPath).show()

        /**
         * +--------+-------+------+
         * |deptName|   name|salary|
         * +--------+-------+------+
         * |  dept-1|Michael|  3000|
         * |  dept-2|   Andy|  5000|
         * |  dept-1|   Alex|  4500|
         * |  dept-2| Justin|  6700|
         * |  dept-2| Cherry|  3400|
         * |  dept-1|   Jack|  5500|
         * |  dept-2|   Jone| 12000|
         * |  dept-1|   Lucy|  8000|
         * |  dept-2|   LiLi|  7600|
         * |  dept-2|   Pony|  4200|
         * +--------+-------+------+
         */

        spark.read.json(userPath).createOrReplaceTempView("user")

        // 实现开窗函数：所谓开窗函数就是分组求TopN
        spark.sql("select deptName,name,salary,rank from" +
                "(select deptName,name,salary,row_number() " +
                "OVER (PARTITION BY deptName order by salary desc) rank from user) tempUser " +
                "where rank <=2").show()

        /**
         * +--------+----+------+----+
         * |deptName|name|salary|rank|
         * +--------+----+------+----+
         * |  dept-1|Lucy|  8000|   1|
         * |  dept-1|Jack|  5500|   2|
         * |  dept-2|Jone| 12000|   1|
         * |  dept-2|LiLi|  7600|   2|
         * +--------+----+------+----+
         */

        // 实现分组排序
        spark.sql("select * from user order by deptName,salary desc").show()

        /**
         * +--------+-------+------+
         * |deptName|   name|salary|
         * +--------+-------+------+
         * |  dept-1|   Lucy|  8000|
         * |  dept-1|   Jack|  5500|
         * |  dept-1|   Alex|  4500|
         * |  dept-1|Michael|  3000|
         * |  dept-2|   Jone| 12000|
         * |  dept-2|   LiLi|  7600|
         * |  dept-2| Justin|  6700|
         * |  dept-2|   Andy|  5000|
         * |  dept-2|   Pony|  4200|
         * |  dept-2| Cherry|  3400|
         * +--------+-------+------+
         */

    }

}
