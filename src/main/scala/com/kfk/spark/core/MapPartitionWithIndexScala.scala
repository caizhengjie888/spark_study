package com.kfk.spark.core

import com.kfk.spark.common.CommSparkContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/29
 * @time : 8:40 下午
 */
object MapPartitionWithIndexScala {
    def main(args: Array[String]): Unit = {
        val sc = CommSparkContextScala.getsc()

        val list = Array("alex", "herry", "lili", "ben", "jack", "jone", "cherry")

        // 将rdd分为3个partition
        val rdd = sc.parallelize(list,3)

        // 查看每个数据对应的分区号
        val indexValues = rdd.mapPartitionsWithIndex((index,x) => {
            var list = List[String]()
            while (x.hasNext){
                val userNameIndex = x.next + " : " + (index + 1)
                list .::= (userNameIndex)
            }
            list.iterator
        })

        indexValues.foreach(x => System.out.println(x))
    }
}
