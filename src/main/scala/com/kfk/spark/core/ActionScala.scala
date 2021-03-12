package com.kfk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/26
 * @time : 10:42 下午
 */
object ActionScala {

    def getsc():SparkContext ={
        val sparkConf = new SparkConf().setAppName("ActionScala").setMaster("local")
        return new SparkContext(sparkConf)
    }

    def main(args: Array[String]): Unit = {

        reduce()
        collect()
        count()
        take()
        save()
        countByKey()
    }

    /**
     * 1,2,3,4,5    reduce() -> 15
     */
    def reduce(): Unit = {

        val list = Array(1,2,3,4,5)

        val rdd = getsc().parallelize(list)

        val reduceValues = rdd.reduce((x,y) => x + y)

        System.out.println(reduceValues)
    }

    /**
     * 1,2,3,4,5    collect() -> [1,2,3,4,5]
     */
    def collect(): Unit ={

        val list = Array(1,2,3,4,5)

        val rdd = getsc().parallelize(list)

        val collectValue = rdd.collect()

        for (value <- collectValue){
            System.out.println(value)
        }
    }

    /**
     * 1,2,3,4,5    count() -> 5
     */
    def count(): Unit ={

        val list = Array(1,2,3,4,5)

        val rdd = getsc().parallelize(list)

        System.out.println(rdd.count())
    }

    /**
     * 1,2,3,4,5    take(3) -> [1,2,3]
     */
    def take(): Unit ={

        val list = Array(1,2,3,4,5)

        val rdd = getsc().parallelize(list)

        val takeValues = rdd.take(3)

        for (value <- takeValues){
            System.out.println(value)
        }
    }

    /**
     * saveAsTextFile()
     */
    def save(): Unit ={

        val list = Array(1,2,3,4,5)

        val rdd = getsc().parallelize(list)

        rdd.saveAsTextFile("hdfs://bigdata-pro-m04:9000/user/caizhengjie/datas/scalaRdd")
    }

    /**
     * <"class_1","alex">
     * <"class_2","jone">
     * <"class_1","lucy">                           <class_1,4>
     * <"class_1","lili">       countByKey() ->
     * <"class_2","ben">                            <class_2,3>
     * <"class_2","jack">
     * <"class_1","cherry">
     */
    def countByKey(): Unit ={

        val list = Array(
            Tuple2("class_1", "alex"),
            Tuple2("class_2", "jack"),
            Tuple2("class_1", "jone"),
            Tuple2("class_1", "lili"),
            Tuple2("class_2", "ben"),
            Tuple2("class_2", "lucy"),
            Tuple2("class_1", "cherry"))

        val rdd = getsc().parallelize(list)

        val countByKeyValues = rdd.countByKey()

        System.out.println(countByKeyValues)
    }
}
