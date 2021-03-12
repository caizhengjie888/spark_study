package com.kfk.spark.core

import com.kfk.spark.common.CommSparkContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/28
 * @time : 6:47 下午
 */
object TopnScala {
    def main(args: Array[String]): Unit = {

        val sc = CommSparkContextScala.getsc()

        val list = Array(34, 54, 32, 12, 67, 25, 84, 58, 39, 18, 39, 81)

        /**
         * 数据模型
         * 34,54,32,12...
         * map()        -> (34,34)(54,54)(32,32)(12,12)
         * sortByKey()  -> (54,54)(34,34)(32,32)(12,12)
         * map()        -> 54,34,32,12
         * take(2)      -> 54,34
         * for()
         */
        val rdd = sc.parallelize(list)
        val beginSortValue = rdd.map(x => (x,x))
        val sortKey = beginSortValue.sortByKey(false)
        val sortMapKey = sortKey.map(x => x._2)
        val sortKeyList = sortMapKey.take(3)
        for (elem <- sortKeyList) {
            System.out.println(elem)
        }
    }
}
