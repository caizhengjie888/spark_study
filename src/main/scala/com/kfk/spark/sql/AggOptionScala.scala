package com.kfk.spark.sql

import com.kfk.spark.common.{Comm, CommSparkSessionScala}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/6
 * @time : 7:29 下午
 */
object AggOptionScala {

    case class Employee(deptId:String,name:String, salary:Long)
    case class Dept(id:String,deptName:String)
    def main(args: Array[String]): Unit = {
        val spark = CommSparkSessionScala.getSparkSession()

        agg1(spark)
        agg2(spark)
    }

    /**
     * avg
     * sum
     * max
     * min
     * count
     * @param spark
     */
    def agg1(spark : SparkSession): Unit ={

        import spark.implicits._
        val employeeDS = spark.read.json(Comm.fileDirPath + "employees1.json").as[Employee]
        val deptDS = spark.read.json(Comm.fileDirPath + "dept.json").as[Dept]

        // join
        val joinDS = employeeDS.join(deptDS,$"deptId" === $"id")
        joinDS.show()

        joinDS.groupBy("deptId","deptName").agg(avg("salary"),sum("salary"),max("salary"),min("salary"),count("name")).show()

        /**
         * +------+--------+-----------+-----------+-----------+-----------+-----------+
         * |deptId|deptName|avg(salary)|sum(salary)|max(salary)|min(salary)|count(name)|
         * +------+--------+-----------+-----------+-----------+-----------+-----------+
         * |dept-1|  开发部|     3500.0|       7000|       4000|       3000|          2|
         * |dept-2|  市场部|     4000.0|       8000|       4500|       3500|          2|
         * +------+--------+-----------+-----------+-----------+-----------+-----------+
         */

        joinDS.groupBy("deptId","deptName").agg("salary" -> "avg","salary" -> "sum","salary" -> "max","salary" -> "min","name" -> "count").show()

    }

    /**
     * collect_list（行转列）将一个分组内指定字段的值收集在一起，不会去重
     * collect_set（行转列）意思同上，唯一的区别就是它会去重
     * @param spark
     */
    def agg2(spark : SparkSession): Unit ={

        import spark.implicits._
        val employeeDS = spark.read.json(Comm.fileDirPath + "employees1.json").as[Employee]
        val deptDS = spark.read.json(Comm.fileDirPath + "dept.json").as[Dept]

        // join
        val joinDS = employeeDS.join(deptDS,$"deptId" === $"id")
        joinDS.show()

        joinDS.groupBy("deptId","deptName").agg(collect_list("name")).show()
        joinDS.groupBy("deptId","deptName").agg(collect_set("name")).show()

        /**
         * +------+--------+-----------------+
         * |deptId|deptName|collect_set(name)|
         * +------+--------+-----------------+
         * |dept-1|  开发部| [Michael, Berta]|
         * |dept-2|  市场部|   [Andy, Justin]|
         * +------+--------+-----------------+
         */

    }
}
