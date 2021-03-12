package com.kfk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/25
 * @time : 7:19 下午
 */
object TransformationScala {

    def getsc():SparkContext ={
        val sparkConf = new SparkConf().setAppName("TransformationScala").setMaster("local")
        return new SparkContext(sparkConf)
    }

    def main(args: Array[String]): Unit = {
        map()
        filter()
        flatmap()
        groupByKey()
        reduceByKey()
        sortByKey()
        join()
        cogroup()
    }

    /**
     * 1,2,3,4,5    map() -> 10,20,30,40,50
     */
    def map(): Unit ={

        val list = Array(1,2,3,4,5)

        val rdd = getsc().parallelize(list)

        val mapValue = rdd.map(x => x * 10)

        mapValue.foreach(x => System.out.println(x))
    }

    /**
     * 1,2,3,4,5,6,7,8,9,10     filter() -> 2,4,6,8,10
     */
    def filter(): Unit ={

        val list = Array(1,2,3,4,5,6,7,8,9,10)

        val rdd = getsc().parallelize(list)

        val filterValue = rdd.filter(x => x % 2 == 0)

        filterValue.foreach(x => System.out.println(x))
    }

    /**
     * hbase hadoop hive
     * java python          flatmap() -> hbase hadoop hive java python java python
     * java python
     */
    def flatmap(): Unit = {

        val list = Array("hbase hadoop hive", "java python", "storm spark")

        val rdd = getsc().parallelize(list)

        val flatMapValue = rdd.flatMap(x => x.split(" "))

        flatMapValue.foreach(x => System.out.println(x))
    }

    /**
     * class_1 90       groupByKey()  -> <class_1,(90,87,98,96)>  <class_2,(95,89,93)>
     * class_2 95
     * class_1 87
     * class_1 98
     * class_2 89
     * class_2 93
     * class_1 96
     */
    def groupByKey(): Unit = {

        val list = Array(
            Tuple2("class_1", 90),
            Tuple2("class_2", 95),
            Tuple2("class_1", 87),
            Tuple2("class_1", 98),
            Tuple2("class_2", 89),
            Tuple2("class_2", 93),
            Tuple2("class_1", 96))

        val rdd = getsc().parallelize(list)

        val groupByKeyValue = rdd.groupByKey()
        reduceByKey()

        groupByKeyValue.foreach(x => {
            System.out.println(x._1)
            x._2.foreach(y => System.out.println(y))
        })
    }

    /**
     * <class_1,(90,87,98,96)>  reduceByKey()  -> <class_1,(90+87+98+96)>
     * <class_2,(95,89,93)>     reduceByKey()  -> <class_2,(95+89+93)>
     */
    def reduceByKey(): Unit = {

        val list = Array(
            Tuple2("class_1", 90),
            Tuple2("class_2", 95),
            Tuple2("class_1", 87),
            Tuple2("class_1", 98),
            Tuple2("class_2", 89),
            Tuple2("class_2", 93),
            Tuple2("class_1", 96))

        val rdd = getsc().parallelize(list)

        val reduceByKeyValues = rdd.reduceByKey((x,y) => x+y)

        reduceByKeyValues.foreach(x => {
            System.out.println(x._1 + " : " + x._2)
        })
    }

    /**
     * <90,alex>
     * <95,lili>     -> <87,cherry> <90,alex> <95,lili>
     * <87,cherry>
     */
    def sortByKey(): Unit ={

        val list = Array(Tuple2(90, "alex"),
            Tuple2(95, "lili"),
            Tuple2(87, "cherry"),
            Tuple2(98, "jack"),
            Tuple2(89, "jone"),
            Tuple2(93, "lucy"),
            Tuple2(96, "aliy")
        )

        val rdd = getsc().parallelize(list)

        val sortByKeyValues = rdd.sortByKey(true)
        sortByKeyValues.foreach(x => {
            System.out.println(x._1 + " : " + x._2)
        })
    }

    /**
     * 数据集一：(1,"alex")   join() -> <1,<"alex",90>>
     * 数据集二：(1,90)
     */
    def join(): Unit ={

        val stuList = Array(Tuple2(1, "alex"),
            Tuple2(2, "lili"),
            Tuple2(3, "cherry"),
            Tuple2(4, "jack"),
            Tuple2(5, "jone"),
            Tuple2(6, "lucy"),
            Tuple2(7, "aliy"))

        val scoreList = Array(Tuple2(1, 90),
            Tuple2(2, 95),
            Tuple2(3, 87),
            Tuple2(4, 98),
            Tuple2(5, 89),
            Tuple2(6, 93),
            Tuple2(7, 96))

        val sc = getsc()

        val stuRdd = sc.parallelize(stuList)

        val scoreRdd = sc.parallelize(scoreList)

        val joinValue = stuRdd.join(scoreRdd)
        joinValue.foreach(x => {
            System.out.println(x._1 + " > " + x._2._1 + " : " + x._2._2)
        })
    }

    /**
     * 数据集一：(2,"lili")                cogroup() -> <2,<"lili",(90,95,99)>>
     * 数据集二：(2,90)(2,95)(2,99)
     */
    def cogroup(): Unit ={

        val stuList = Array(Tuple2(1, "alex"),
            Tuple2(2, "lili"),
            Tuple2(3, "cherry"),
            Tuple2(4, "jack"),
            Tuple2(5, "jone"),
            Tuple2(6, "lucy"),
            Tuple2(7, "aliy"))

        val scoreList = Array(Tuple2(1, 90),
            Tuple2(2, 95),
            Tuple2(2, 95),
            Tuple2(2, 99),
            Tuple2(3, 87),
            Tuple2(3, 88),
            Tuple2(3, 89),
            Tuple2(4, 98),
            Tuple2(5, 89),
            Tuple2(6, 93),
            Tuple2(7, 96))

        val sc = getsc()

        val stuRdd = sc.parallelize(stuList)

        val scoreRdd = sc.parallelize(scoreList)

        val cogroupValues = stuRdd.cogroup(scoreRdd)
        cogroupValues.foreach(x => {
            System.out.println(x._1 + " > " + x._2._1.toList + " : " + x._2._2.toList)
        })
    }
}
