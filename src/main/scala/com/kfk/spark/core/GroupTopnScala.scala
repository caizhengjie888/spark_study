package com.kfk.spark.core

import com.kfk.spark.common.CommSparkContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/28
 * @time : 8:38 下午
 */
object GroupTopnScala {
    def main(args: Array[String]): Unit = {
        val sc = CommSparkContextScala.getsc()
        val list = Array("class1 90",
            "class2 93",
            "class1 97",
            "class1 89",
            "class3 99",
            "class1 34",
            "class1 45",
            "class1 99",
            "class3 78",
            "class1 79",
            "class2 85",
            "class2 89",
            "class2 96",
            "class3 92",
            "class1 98",
            "class3 86")

        val rdd = sc.parallelize(list)

        /**
         * class1 90       mapToPair() -> (class1,90)
         * class2 93
         * class1 97
         * class1 89
         * ...
         */
        val beginGroupValue = rdd.map(x => {
            val key = x.split(" ")(0)
            val value = x.split(" ")(1).toInt
            (key,value)
        })

        /**
         * <class1,(90,97,98,89,79,34,45,99)>
         * ...
         */
        val groupValues = beginGroupValue.groupByKey()

        /**
         * <class1,(90,97,98,89,79,34,45,99)>      map() -> <class1,(99,98,97,90,89,79,45,34)>
         */
        val groupTopValues = groupValues.map(x => {
            val values = x._2.toList.sortWith((x,y) => x>y).take(3)
            (x._1,values)
        })
        groupTopValues.foreach(x => {
            System.out.println(x._1)
            for (elem <- x._2) {
                System.out.println(elem)
            }
        })
    }
}
