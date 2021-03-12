package com.kfk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/28
 * @time : 1:21 下午
 */
object SecondSortScala {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("wordCountApp").setMaster("local")
        val sc = new SparkContext(sparkConf)

        /**
         * class1 90
         * class2 93
         * class1 97        ->  class1 (89,90,97...) class2(89,93...) class3(86,99...)
         * class1 89
         * class3 99
         * ...
         */
        val list = Array("class1 90", "class2 93", "class1 97",
            "class1 89", "class3 99", "class3 78",
            "class1 79", "class2 85", "class2 89",
            "class2 96", "class3 92", "class3 86")

        val rdd = sc.parallelize(list)

        val beginSortValues = rdd.map(x => (new SecondSortKeyScala(x.split(" ")(0),x.split(" ")(1).toInt),x))

        val sortValues = beginSortValues.sortByKey(false)

        sortValues.foreach(x => System.out.println(x._2))
    }
}
