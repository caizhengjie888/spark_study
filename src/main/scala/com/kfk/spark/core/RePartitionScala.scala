package com.kfk.spark.core

import com.kfk.spark.common.CommSparkContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/29
 * @time : 10:20 下午
 */
object RePartitionScala {
    def main(args: Array[String]): Unit = {
        val sc = CommSparkContextScala.getsc()
        val list = Array("alex", "herry", "lili", "ben", "jack", "jone", "cherry", "lucy", "pony", "leo")

        val rdd = sc.parallelize(list,2)

        // 查看每个值对应每个分区
        val indexValues1 = rdd.mapPartitionsWithIndex((index,x) => {
            var list = List[String]()
            while (x.hasNext){
                val indexStr = x.next() + " " + "以前分区" + " : " + (index + 1)
                list .::= (indexStr)
            }
            list.iterator
        })

        // 增加为四个分区
        val repartitionValues = indexValues1.repartition(4)

        // 增加四个分区之后查看每个值对应每个分区
        val indexValues2 = repartitionValues.mapPartitionsWithIndex((index,y) => {
            var list = List[String]()
            while (y.hasNext){
                val indexStr = y.next() + " " + "现在分区" + " : " + (index + 1)
                list .::= (indexStr)
            }
            list.iterator
        })

        indexValues2.foreach(x => System.out.println(x))
    }
}
