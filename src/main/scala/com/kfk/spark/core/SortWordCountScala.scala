package com.kfk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/22
 * @time : 10:48 下午
 */
object SortWordCountScala {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("wordCountApp").setMaster("local")
        val sc = new SparkContext(sparkConf)
        val lines = sc.textFile("hdfs://bigdata-pro-m04:9000/user/caizhengjie/datas/wordcount.txt")

        // val wordcount = lines.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((x,y) => (x+y)).foreach((_wordcount => println(_wordcount._1 + " : " + _wordcount._2))

        /**
         * java python hive     flatMap() -> java python hive hive java...
         * hive java ...
         */
        val words = lines.flatMap(line => line.split(" "))

        /**
         * java python hive hive java...    map() -> (java,1)(hive,1)(java,1)(python,1)...
         */
        val word = words.map(word => (word, 1))

        /**
         * (java,1)(hive,1)(java,1)(python,1)...    reduceByKey() -> (java,2)(hive,1)(python,1)...
         */
        val wordcount = word.reduceByKey((x, y) => x + y)

        /**
         * (spark,1)(hive,3)(hadoop,3)...  map() -> (3,hadoop)(3,hive)...
         */
        val wordcountSortValue = wordcount.map(x => (x._2,x._1))

        /**
         * (3,hadoop)(3,hive)...    sortByKey(false) -> (3,hadoop)(3,hive)(2,java)(1,python)...
         */
        val sort = wordcountSortValue.sortByKey(false)

        /**
         * (3,hadoop)(3,hive)(2,java)(1,python)...      map() -> (hadoop,3)(hive,3)(java,2)(python,1)...
         */
        val wordcountSortValues = sort.map(x => (x._2,x._1))

        /**
         * foreach()
         */
        wordcountSortValues.foreach(_wordcount => println(_wordcount._1 + " : " + _wordcount._2))
    }
}
