package com.kfk.spark.sql;

import com.kfk.spark.common.Comm;
import com.kfk.spark.common.CommSparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/3
 * @time : 10:47 下午
 */
public class LoadAndSaveJava {
    public static void main(String[] args) {
        SparkSession spark = CommSparkSession.getSparkSession();

        // load
        Dataset<Row> usersDF = spark.read().load(Comm.fileDirPath + "users.parquet");

        //save
        usersDF.select("name","favorite_color","favorite_numbers").write().save(Comm.saveFileDirPath + "test.parquet");
        usersDF.select("name","favorite_color","favorite_numbers").write().format("json").save(Comm.saveFileDirPath + "test.json");

    }
}
