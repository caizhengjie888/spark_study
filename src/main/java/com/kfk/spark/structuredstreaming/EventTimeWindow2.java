package com.kfk.spark.structuredstreaming;

import com.kfk.spark.common.CommSparkSession;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;


import java.sql.Timestamp;


/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/22
 * @time : 9:44 下午
 */
public class EventTimeWindow2 {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = CommSparkSession.getSparkSession();

        // input table
        Dataset<String> dflines = spark.readStream()
                .format("socket")
                .option("host", "bigdata-pro-m04")
                .option("port", 9999)
                .load().as(Encoders.STRING());

        /**
         *  数据源：2019-03-29 16:40:00,hadoop
         *        2019-03-29 16:40:10,storm
         *
         *  // 根据javaBean转换为dataset
         */
        Dataset<EventData> dfeventdata = dflines.map(new MapFunction<String, EventData>() {
            @Override
            public EventData call(String value) throws Exception {

                String[] lines  = value.split(",");
                return new EventData(Timestamp.valueOf(lines[0]),lines[1]);
            }
        }, ExpressionEncoder.javaBean(EventData.class));

        // result table
        Dataset<Row> windowedCounts = dfeventdata.groupBy(
                functions.window(dfeventdata.col("wordtime"),
                        "10 minutes",
                        "5 minutes"),
                dfeventdata.col("word")
        ).count();

        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate","false")
                .start();
        query.awaitTermination();

    }
}
