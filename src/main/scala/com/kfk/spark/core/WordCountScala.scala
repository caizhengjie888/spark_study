package com.kfk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/22
 * @time : 10:48 下午
 */
object WordCountScala {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("wordCountApp").setMaster("local")
        val sc = new SparkContext(sparkConf)
        val lines = sc.textFile("hdfs://bigdata-pro-m04:9000/user/caizhengjie/datas/wordcount.txt")

        // val wordcount = lines.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((x,y) => (x+y)).foreach((_wordcount => println(_wordcount._1 + " : " + _wordcount._2))

        val words = lines.flatMap(line => line.split(" "))
        val word = words.map(word => (word, 1))
        val wordcount = word.reduceByKey((x, y) => (x + y))

        // println(wordcount.collect().toList)

        wordcount.foreach(_wordcount => println(_wordcount._1 + " : " + _wordcount._2))
    }
}
