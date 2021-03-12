package com.kfk.spark.core

import com.kfk.spark.common.CommSparkContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/30
 * @time : 9:42 上午
 */
object SampleScala {
    def main(args: Array[String]): Unit = {
        val sc = CommSparkContextScala.getsc()

        val list = Array("alex", "herry", "lili", "ben", "jack", "jone", "cherry")

        val rdd = sc.parallelize(list)

        // 随机抽取50%
        val sampleValues  =rdd.sample(false,0.5)

        for (elem <- sampleValues.collect()) {
            System.out.println(elem)
        }
    }
}
