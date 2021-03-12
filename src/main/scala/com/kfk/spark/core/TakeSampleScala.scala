package com.kfk.spark.core

import com.kfk.spark.common.CommSparkContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/29
 * @time : 10:26 下午
 */
object TakeSampleScala {
    def main(args: Array[String]): Unit = {
        val sc = CommSparkContextScala.getsc()
        val list = Array("alex", "herry", "lili", "ben", "jack", "jone", "cherry", "lucy", "pony", "leo")
        val rdd = sc.parallelize(list,2)

        // 随机抽取3个数据
        val takeSampleList = rdd.takeSample(false,3)

        for (elem <- takeSampleList) {
            System.out.println(elem)
        }
    }
}
