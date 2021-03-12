package com.kfk.spark.core

import com.kfk.spark.common.CommSparkContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/29
 * @time : 9:00 下午
 */
object CartesionScala {

    def main(args: Array[String]): Unit = {
        val sc = CommSparkContextScala.getsc()
        val list1 = Array("衣服-1", "衣服-2")
        val list2 = Array("裤子-1", "裤子-2")

        val rdd1 = sc.parallelize(list1)
        val rdd2 = sc.parallelize(list2)

        /**
         * (cherry,alex)(cherry,jack)(herry,alex)(herry,jack)
         */
        val carteValues = rdd1.cartesian(rdd2)

        carteValues.foreach(x => System.out.println(x))
    }
}
