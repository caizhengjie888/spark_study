package com.kfk.spark.structuredstreaming;

import com.kfk.spark.common.CommSparkSession;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.sql.Date;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/21
 * @time : 10:35 下午
 */
public class StruStreamingDFOper {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = CommSparkSession.getSparkSession();

        // input table
        Dataset<String> dflines = spark.readStream()
                .format("socket")
                .option("host", "bigdata-pro-m04")
                .option("port", 9999)
                .load().as(Encoders.STRING());

        // 根据javaBean转换为dataset
        Dataset<DeviceData> dfdevice = dflines.map(new MapFunction<String, DeviceData>() {
            @Override
            public DeviceData call(String value) throws Exception {
                String[] lines = value.split(",");

                return new DeviceData(lines[0],lines[1],Double.parseDouble(lines[2]), Date.valueOf(lines[3]));
            }
        }, ExpressionEncoder.javaBean(DeviceData.class));

        // result table
        Dataset<Row> dffinal = dfdevice.select("device","deviceType").where("signal > 10").groupBy("deviceType").count();

        // output
        StreamingQuery query = dffinal.writeStream()
                .outputMode("update")
                .format("console")
                .start();

        query.awaitTermination();

    }
}
