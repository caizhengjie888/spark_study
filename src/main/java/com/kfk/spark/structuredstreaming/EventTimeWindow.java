package com.kfk.spark.structuredstreaming;

import com.kfk.spark.common.CommSparkSession;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/22
 * @time : 9:44 下午
 */
public class EventTimeWindow {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = CommSparkSession.getSparkSession();

        // input table
        Dataset<Row> dflines = spark.readStream()
                .format("socket")
                .option("host", "bigdata-pro-m04")
                .option("port", 9999)
                .option("includeTimestamp",true)
                .load();

        /**
         * 输入数据：spark storm hive
         * 接受到数据模型：
         * 第一步：words -> spark storm hive,  timestamp -> 2019-09-09 12:12:12
         *        eg: tuple -> (spark storm hive , 2019-09-09 12:12:12)
         *
         * 第二步：split()操作
         *        对tuple中的key进行split操作
         *
         * 第三步：flatMap()操作
         * 返回类型 -> list(tuple(spark,2019-09-09 12:12:12),
         *               tuple(storm,2019-09-09 12:12:12),
         *               tuple(hive,2019-09-09 12:12:12))
         */
        Dataset<Row> words =  dflines.as(Encoders.tuple(Encoders.STRING(),Encoders.TIMESTAMP()))
                .flatMap(new FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>() {
                    @Override
                    public Iterator<Tuple2<String, Timestamp>> call(Tuple2<String, Timestamp> stringTimestampTuple2) throws Exception {
                        List<Tuple2<String, Timestamp>> result = new ArrayList<>();
                        for (String word : stringTimestampTuple2._1.split(" ")){
                            result.add(new Tuple2<>(word,stringTimestampTuple2._2));
                        }
                        return result.iterator();
                    }
                },Encoders.tuple(Encoders.STRING(),Encoders.TIMESTAMP())).toDF("word","wordtime");

        // result table
        Dataset<Row> windowedCounts = words.groupBy(
                functions.window(words.col("wordtime"),
                        "10 minutes",
                        "5 minutes"),
                words.col("word")
        ).count();

        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate","false")
                .start();
        query.awaitTermination();

    }
}
