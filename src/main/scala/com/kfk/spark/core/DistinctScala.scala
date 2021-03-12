package com.kfk.spark.core

import com.kfk.spark.common.CommSparkContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/30
 * @time : 10:06 上午
 */
object DistinctScala {
    def main(args: Array[String]): Unit = {
        val sc = CommSparkContextScala.getsc()

        val list = Array("alex", "herry", "lili", "ben", "jack", "alex", "herry")

        val rdd = sc.parallelize(list)

        // 对RDD中的数据进行去重
        val distinctValues = rdd.distinct()

        for (elem <- distinctValues.collect()) {
            System.out.println(elem)
        }
    }
}
