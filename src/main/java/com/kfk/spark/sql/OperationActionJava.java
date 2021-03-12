package com.kfk.spark.sql;

import com.kfk.spark.common.Comm;
import com.kfk.spark.common.CommSparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/4
 * @time : 11:27 上午
 */
public class OperationActionJava {
    public static void main(String[] args) {
        SparkSession spark = CommSparkSession.getSparkSession();

        String path = Comm.fileDirPath + "people.json";

        Dataset<Row> df = spark.read().json(path);
        df.show();

        for (Row row : df.collectAsList()) {
            System.out.println(row);
        }

        System.out.println(df.first());

        for (Row row : df.takeAsList(2)) {
            System.out.println(row);
        }
    }
}
