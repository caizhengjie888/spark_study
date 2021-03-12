package com.kfk.spark.blacklist_project

import com.kfk.spark.common.CommStreamingContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/16
 * @time : 10:31 下午
 */
object BlackListScala {
    def main(args: Array[String]): Unit = {
        val jssc = CommStreamingContextScala.getJssc

        /**
         * 模拟创建黑名单
         * 数据格式：<String,Boolean> -> <"jack",true>
         */
        val blacklist = Array(("ben",true),("leo",true))

        val blacklistRdd = jssc.sparkContext.parallelize(blacklist)

        // nc数据源
        val userDstream = jssc.socketTextStream("bigdata-pro-m04", 9999)

        /**
         * 源数据：2019-09-09 张三    -> 转换后的数据：  <"张三",2019-09-09 张三>
         */
        val pair = userDstream.map(line => (line.split(" ")(1),line))

        /**
         * <"张三",2019-09-09 张三> <"张三",true> -> leftOuterJoin -> <”张三”,<张三 2019-09-09,true>> -> filter 过滤掉==true的数据
         */
        val vailStream = pair.transform(tuple => {
            val leftJoinRDD = tuple.leftOuterJoin(blacklistRdd)
            val filterRDD = leftJoinRDD.filter(rdd => {
                if (rdd._2._2.getOrElse(0) == true) false else true
            })

            val valiRDD = filterRDD.map(tuple => {
                tuple._1 + " : " + tuple._2._1 + " : " + tuple._2._2
            })
            valiRDD
        })

        vailStream.print()

        jssc.start()
        jssc.awaitTermination()
    }
}
