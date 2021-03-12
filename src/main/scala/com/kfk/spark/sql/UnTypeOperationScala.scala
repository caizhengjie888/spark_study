package com.kfk.spark.sql

import com.kfk.spark.common.{Comm, CommSparkSessionScala}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/6
 * @time : 3:39 下午
 */
object UnTypeOperationScala {
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
         * +------+-------+------+--------+------+
         * |deptId|   name|salary|deptName|    id|
         * +------+-------+------+--------+------+
         * |dept-1|Michael|  3000|  开发部|dept-1|
         * |dept-2|   Andy|  4500|  市场部|dept-2|
         * |dept-2| Justin|  3500|  市场部|dept-2|
         * |dept-1|  Berta|  4000|  开发部|dept-1|
         * +------+-------+------+--------+------+
         */

        // 使用算子
        joinDS.groupBy("deptId","deptName").sum("salary").select("deptId","deptName","sum(salary)").orderBy($"sum(salary)".desc).show()

        /**
         * +------+--------+-----------+
         * |deptId|deptName|sum(salary)|
         * +------+--------+-----------+
         * |dept-2|  市场部|       8000|
         * |dept-1|  开发部|       7000|
         * +------+--------+-----------+
         */

        // 创建临时表，使用sql语句
        joinDS.createOrReplaceTempView("deptSalary")
        val resultDF = spark.sql("select a.deptId, a.deptName ,sum(salary) as salary from deptSalary a group by a.deptId, a.deptName order by salary desc")
        resultDF.show()

        /**
         * +------+--------+------+
         * |deptId|deptName|salary|
         * +------+--------+------+
         * |dept-2|  市场部|  8000|
         * |dept-1|  开发部|  7000|
         * +------+--------+------+
         */

    }
}
