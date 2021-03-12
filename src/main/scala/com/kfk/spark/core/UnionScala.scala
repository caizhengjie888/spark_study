package com.kfk.spark.core

import com.kfk.spark.common.CommSparkContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/30
 * @time : 9:55 上午
 */
object UnionScala {
    def main(args: Array[String]): Unit = {
        val sc = CommSparkContextScala.getsc()

        val list1 = Array("alex", "herry", "lili", "ben", "jack")
        val list2 = Array("jone", "cherry", "lucy", "pony", "leo")

        val rdd1 = sc.parallelize(list1)
        val rdd2 = sc.parallelize(list2)

        // 将两个RDD的数据合并为一个RDD
        val sampleValues = rdd1.union(rdd2)

        for (elem <- sampleValues.collect()) {
            System.out.println(elem)
        }
    }
}
