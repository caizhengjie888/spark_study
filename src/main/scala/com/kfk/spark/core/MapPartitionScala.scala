package com.kfk.spark.core

import com.kfk.spark.common.CommSparkContextScala
import org.apache.spark.SparkContext

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/29
 * @time : 4:59 下午
 */
object MapPartitionScala {
    def main(args: Array[String]): Unit = {
        val sc = CommSparkContextScala.getsc()
        val list = Array("alex", "herry", "lili", "ben", "jack", "jone", "cherry")
        val map = Map("alex" -> 98.6,"herry" -> 89.5,"lili" -> 87.3,"ben" -> 91.2,"jack" -> 78.9,"jone" -> 95.4,"cherry" -> 96.1)

        // 将rdd分为2个partition
        val rdd = sc.parallelize(list,2)

        // 对每一个分区中的数据同时做处理
        val mapPartitionValues = rdd.mapPartitions(x => {
            var list = List[Double]()
            while (x.hasNext){
                val userName = x.next()
                val score = map.get(userName).get
                list .::= (score)
            }
            list.iterator
        })

        mapPartitionValues.foreach(x => System.out.println(x))
    }
}
