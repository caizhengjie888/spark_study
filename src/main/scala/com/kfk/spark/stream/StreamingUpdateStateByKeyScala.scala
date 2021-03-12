package com.kfk.spark.stream

import com.kfk.spark.common.CommStreamingContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/16
 * @time : 8:04 下午
 */
object StreamingUpdateStateByKeyScala {
    def main(args: Array[String]): Unit = {
        val jssc = CommStreamingContextScala.getJssc;

        val lines = jssc.socketTextStream("bigdata-pro-m04", 9999)

        // 要使用UpdateStateByKey算子就必须设置一个Checkpoint目录，开启Checkpoint机制
        // 以便于内存数据丢失时，可以从Checkpoint中恢复数据
        jssc.checkpoint("hdfs://bigdata-pro-m04:9000/user/caizhengjie/datas/sparkCheckpoint")

        // flatmap
        val words = lines.flatMap(word => word.split(" "))

        // map
        val pair = words.map(x => (x,1))

        // updateStateByKey
        val wordcount = pair.updateStateByKey((values : Seq[Int], state : Option[Int]) => {
            var newValue = state.getOrElse(0)

            for (value <- values){
                newValue += value
            }
            Option(newValue)
        })

        wordcount.print()

        jssc.start()
        jssc.awaitTermination()
    }

}
