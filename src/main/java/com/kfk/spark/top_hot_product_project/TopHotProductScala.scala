package com.kfk.spark.top_hot_product_project

import com.kfk.spark.common.{CommSparkSessionScala, CommStreamingContextScala}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.Seconds

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/19
 * @time : 3:59 下午
 */
object TopHotProductScala {
    def main(args: Array[String]): Unit = {
        val jssc = CommStreamingContextScala.getJssc

        /**
         * 输入数据源模型
         * --------------------------
         * 姓名		商品分类		商品名称
         * alex 	bigdata		hadoop
         * alex 	bigdata		spark
         * jack 	bigdata		flink
         * alex 	bigdata		hadoop
         * alex 	language	java
         * pony 	language	python
         * jone 	language	java
         * lili 	bigdata		hadoop
         * --------------------------
         */
        val inputDstream = jssc.socketTextStream("bigdata-pro-m04", 9999)

        /**
         * mapToPair
         * <bigdata_hadoop,1>
         * <bigdata_spark,1>
         * <bigdata_flink,1>
         */
        val pairDStream = inputDstream.map(x => (x.split(" ")(1) + "_" + x.split(" ")(2),1))

        /**
         * reduceByKeyAndWindow
         * <bigdata_hadoop,5>
         * <bigdata_spark,2>
         * <bigdata_flink,4>
         */
        val windowDStream = pairDStream.reduceByKeyAndWindow((x:Int,y:Int) => x+y,Seconds(60),Seconds(10))

        windowDStream.foreachRDD(x => {

            // 转换成RDD[Row]
            val rowRDD = x.map(tuple => {
                val category = tuple._1.split("_")(0)
                val product = tuple._1.split("_")(1)
                val productCount = tuple._2
                Row(category,product,productCount)
            })

            // 构造Schema
            val structType = StructType(Array(
                StructField("category",StringType,true),
                StructField("product",StringType,true),
                StructField("productCount",IntegerType,true)
            ))

            // 创建SparkSession
            val spark = CommSparkSessionScala.getSparkSession()

            // 通过rdd和scheme创建DataFrame
            val df = spark.createDataFrame(rowRDD,structType)

            // 创建临时表
            df.createOrReplaceTempView("product_click")

            /**
             * 统计每个种类下点击次数排名前3名的商品
             */
            spark.sql("select category,product,productCount from product_click "
                    + "order by productCount desc limit 3").show()

            /**
             * +--------+-------+------------+
             * |category|product|productCount|
             * +--------+-------+------------+
             * | bigdata| hadoop|           4|
             * | bigdata|  kafka|           2|
             * | bigdata|  flink|           2|
             * +--------+-------+------------+
             */
        })

        jssc.start()
        jssc.awaitTermination()

    }

}
