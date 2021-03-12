package com.kfk.spark.sql

import com.kfk.spark.common.{Comm, CommSparkSessionScala}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/7
 * @time : 7:18 下午
 */
object ParquetScala {
    def main(args: Array[String]): Unit = {
        val spark = CommSparkSessionScala.getSparkSession()

        basicLoad(spark)
        partition(spark)
        mege1(spark)
        mege2(spark)
    }

    // 数据的加载方式
    def basicLoad(spark : SparkSession): Unit ={

        import spark.implicits._
        val jsonPath = Comm.fileDirPath + "people.json"
        val parquetPath = Comm.fileDirPath + "people.parquet"

        val peopleDF = spark.read.json(jsonPath)

        // 将json文件转换成parquet文件
        // peopleDF.write.parquet(Comm.fileDirPath + "people.parquet")

        val parquetDF = spark.read.parquet(parquetPath).createOrReplaceTempView("people")
        spark.sql("select * from people").map(row => "mame : " + row.getAs("name")).show()

    }

    // 数据源的自动分区
    def partition(spark : SparkSession): Unit ={
        import spark.implicits._

        val partitionPath = Comm.fileDirPath + "people"
        spark.read.parquet(partitionPath).show()

        /**
         * +----+-------+------+-------+
         * | age|   name|gender|country|
         * +----+-------+------+-------+
         * |null|Michael|  male|     US|
         * |  30|   Andy|  male|     US|
         * |  19| Justin|  male|     US|
         * |  19| Justin|  male|     US|
         * |  32| Justin|  male|     US|
         * +----+-------+------+-------+
         */
    }

    // 数据源的元数据合并一
    def mege1(spark : SparkSession): Unit ={
        import spark.implicits._

        val squareDF = spark.sparkContext.makeRDD(1 to 5).map(x => (x , x * x)).toDF("value","square")
        squareDF.write.parquet(Comm.fileDirPath + "test_table/key=1")

        val cubeDF = spark.sparkContext.makeRDD(6 to 10).map(x => (x , x * x * x)).toDF("value","cube")
        cubeDF.write.parquet(Comm.fileDirPath + "test_table/key=2")

        val megeDF = spark.read.option("mergeSchema",true).parquet(Comm.fileDirPath + "test_table").printSchema()

        /**
         * root
             |-- value: integer (nullable = true)
             |-- square: integer (nullable = true)
             |-- cube: integer (nullable = true)
             |-- key: integer (nullable = true)
         */

    }

    // 数据源的元数据合并二
    def mege2(spark : SparkSession): Unit ={

        import spark.implicits._

        val array1 = Array(("alex",22),("cherry",23))
        val arrayDF1 = spark.sparkContext.makeRDD(array1.toSeq).toDF("name","age").write.format("parquet").mode(SaveMode.Append).save(Comm.fileDirPath + "test_table2")

        val array2 = Array(("alex","dept-1"),("cherry","dept-2"))
        val arrayDF2 = spark.sparkContext.makeRDD(array2.toSeq).toDF("name","dept").write.format("parquet").mode(SaveMode.Append).save(Comm.fileDirPath + "test_table2")

        val megeDF = spark.read.option("mergeSchema",true).parquet(Comm.fileDirPath + "test_table2").printSchema()

        /**
         * +------+------+----+
         * |  name|  dept| age|
         * +------+------+----+
         * |  alex|dept-1|null|
         * |cherry|dept-2|null|
         * |  alex|  null|  22|
         * |cherry|  null|  23|
         * +------+------+----+
         *
         * root
             |-- name: string (nullable = true)
             |-- dept: string (nullable = true)
             |-- age: integer (nullable = true)
         */

    }
}
