package com.kfk.spark.structuredstreaming;

import com.kfk.spark.common.CommSparkSession;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/21
 * @time : 10:35 下午
 */
public class StruStreamingDFOper2 {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = CommSparkSession.getSparkSession();

        // input table
        Dataset<String> dflines = spark.readStream()
                .format("socket")
                .option("host", "bigdata-pro-m04")
                .option("port", 9999)
                .load().as(Encoders.STRING());

        List<StructField> fields = new ArrayList<StructField>();

        StructField device = DataTypes.createStructField("device",DataTypes.StringType,true);
        StructField deviceType = DataTypes.createStructField("deviceType",DataTypes.StringType,true);
        StructField signal = DataTypes.createStructField("signal",DataTypes.DoubleType,true);
        StructField deviceTime = DataTypes.createStructField("deviceTime",DataTypes.DateType,true);

        fields.add(device);
        fields.add(deviceType);
        fields.add(signal);
        fields.add(deviceTime);

        // 构造Schema
        StructType scheme = DataTypes.createStructType(fields);

        // 根据schema转换为dataset
        Dataset<Row> dfdevice = dflines.map(new MapFunction<String, Row>() {
            @Override
            public Row call(String value) throws Exception {
                String[] lines = value.split(",");

                return RowFactory.create(lines[0],lines[1],Double.parseDouble(lines[2]), Date.valueOf(lines[3]));
            }
        }, RowEncoder.apply(scheme));

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
