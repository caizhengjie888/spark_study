package com.kfk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/27
 * @time : 9:00 下午
 */
object VarScala {
    def getsc():SparkContext ={
        val sparkConf = new SparkConf().setAppName("VarScala").setMaster("local")
        return new SparkContext(sparkConf)
    }
    def main(args: Array[String]): Unit = {

        val list = Array(1,2,3,4,5)
        val sc = getsc()
        // 广播变量
        val broadcast = sc.broadcast(10)
        val rdd = sc.parallelize(list)

        val values = rdd.map(x => x * broadcast.value)
        values.foreach(x => System.out.println(x))

        // 累加变量
        val accumulator = sc.longAccumulator
        val mapValue = rdd.foreach(x => {
            accumulator.add(x)
        })

        System.out.println(accumulator.value)
    }
}
