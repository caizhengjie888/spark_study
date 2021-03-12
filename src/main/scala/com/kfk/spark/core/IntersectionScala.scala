package com.kfk.spark.core

import com.kfk.spark.common.CommSparkContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/30
 * @time : 10:01 上午
 */
object IntersectionScala {
    def main(args: Array[String]): Unit = {
        val sc = CommSparkContextScala.getsc()

        val list1 = Array("alex", "herry", "lili", "ben", "jack")
        val list2 = Array("jone", "alex", "lili", "pony", "leo")

        val rdd1 = sc.parallelize(list1)
        val rdd2 = sc.parallelize(list2)

        // 获取两个RDD相同的数据
        val intersectionValues = rdd1.intersection(rdd2)
        for (elem <- intersectionValues.collect()) {
            System.out.println(elem)
        }
    }
}
