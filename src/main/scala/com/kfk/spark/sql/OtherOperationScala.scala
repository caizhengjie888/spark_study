package com.kfk.spark.sql

import com.kfk.spark.common.{Comm, CommSparkSessionScala}
import org.apache.spark.sql.functions._

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/6
 * @time : 8:13 下午
 */
object OtherOperationScala {
    case class Employee(deptId:String,name:String, salary:Long)
    case class Dept(id:String,deptName:String)
    def main(args: Array[String]): Unit = {

        val spark = CommSparkSessionScala.getSparkSession()
        import spark.implicits._
        val employeeDS = spark.read.json(Comm.fileDirPath + "employees1.json").as[Employee]
        val deptDS = spark.read.json(Comm.fileDirPath + "dept.json").as[Dept]

        // join
        val joinDS = employeeDS.join(deptDS,$"deptId" === $"id")
        joinDS.show()

        /**
         * 日期函数:current_date current_timestamp
         * 数学函数:round
         * 随机函数:rand
         * 字符串函数:concat
         */

        joinDS.select(current_date(),current_timestamp(),rand(),round($"salary",2),concat($"name",$"salary")).show()

        /**
         * +--------------+--------------------+-------------------------+----------------+--------------------+
         * |current_date()| current_timestamp()|rand(7003525560546994152)|round(salary, 2)|concat(name, salary)|
         * +--------------+--------------------+-------------------------+----------------+--------------------+
         * |    2020-12-06|2020-12-06 21:13:...|       0.8157198990085353|            3000|         Michael3000|
         * |    2020-12-06|2020-12-06 21:13:...|       0.8446053083722802|            4500|            Andy4500|
         * |    2020-12-06|2020-12-06 21:13:...|       0.5323697131017762|            3500|          Justin3500|
         * |    2020-12-06|2020-12-06 21:13:...|      0.10113425439292889|            4000|           Berta4000|
         * +--------------+--------------------+-------------------------+----------------+--------------------+
         */
    }

}
