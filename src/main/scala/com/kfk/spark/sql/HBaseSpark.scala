package com.kfk.spark.sql

import com.kfk.spark.common.CommSparkSessionScala
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/10
 * @time : 8:53 下午
 */
object HBaseSpark {
    def main(args: Array[String]): Unit = {

        // configuration
        val spark = CommSparkSessionScala.getSparkSession()
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
        hbaseConf.set("hbase.zookeeper.quorum","bigdata-pro-m04")
        hbaseConf.set("hbase.master","bigdata-pro-m04:60010")

        // get
        getHBase(hbaseConf,spark)

        // write
        writeHBase(hbaseConf,spark)

    }

    /**
     * 读取hbase中的数据
     * @param hbaseConf
     * @param spark
     */
    def getHBase(hbaseConf : Configuration,spark : SparkSession): Unit ={

        // 获取表名
        hbaseConf.set(TableInputFormat.INPUT_TABLE,"stu")

        // 将hbase中的数据转换成rdd
        val hbaseRDD =  spark.sparkContext.newAPIHadoopRDD(hbaseConf,
            classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        // 打印数据
        hbaseRDD.foreach(result => {
            val key = Bytes.toString(result._2.getRow)
            val addres = Bytes.toString(result._2.getValue("info".getBytes(),"addres".getBytes()))
            val age = Bytes.toString(result._2.getValue("info".getBytes(),"age".getBytes()))
            val username = Bytes.toString(result._2.getValue("info".getBytes(),"username".getBytes()))

            println("row key:" + key + " addres=" + addres + " age=" + age + " username=" + username)hashCode()

            /**
             * row key:001 addres=guangzhou age=20 username=alex
             * row key:002 addres=shenzhen age=34 username=jack
             * row key:003 addres=beijing age=23 username=lili
             */

        })
    }

    /**
     * 将数据写入到hbase
     * @param hbaseConf
     * @param spark
     */
    def writeHBase(hbaseConf : Configuration,spark : SparkSession): Unit ={

        // 初始化job，设置输出格式，TableOutputFormat 是 org.apache.hadoop.hbase.mapred 包下的
        val jobConf = new JobConf(hbaseConf)

        jobConf.setOutputFormat(classOf[TableOutputFormat])

        // 获取表名
        jobConf.set(TableOutputFormat.OUTPUT_TABLE,"stu")

        // 准备数据
        val array = Array("004,shanghai,25,jone",
                          "005,nanjing,31,cherry",
                          "006,wuhan,18,pony")

        val rdd = spark.sparkContext.makeRDD(array)

        // 将写入到hbase的数据转换成rdd
        val saveRDD = rdd.map(line => line.split(",")).map(x => {

            /**
             * 一个Put对象就是一行记录，在构造方法中指定主键
             * 所有插入的数据 须用 org.apache.hadoop.hbase.util.Bytes.toBytes 转换
             * Put.addColumn 方法接收三个参数：列族，列名，数据
             */
            val put = new Put(Bytes.toBytes(x(0)))
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("addres"),Bytes.toBytes(x(1)))
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(x(2)))
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("username"),Bytes.toBytes(x(3)))
            (new ImmutableBytesWritable,put)
        })

        // 写入到hbase中
        saveRDD.saveAsHadoopDataset(jobConf)

    }
}
