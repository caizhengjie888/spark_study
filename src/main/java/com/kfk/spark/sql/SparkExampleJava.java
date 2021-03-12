package com.kfk.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/2
 * @time : 8:15 下午
 */
public class SparkExampleJava {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("SparkExample")
                .master("local")
                .config("spark.sql.warehouse.dir","/Users/caizhengjie/Document/spark/spark-warehouse")
                .getOrCreate();


        String path = "/Users/caizhengjie/IdeaProjects/spark_study01/src/main/resources/datas/people.json";
        Dataset<Row> df = spark.read().json(path);

        // Displays the content of the DataFrame to stdout
        df.show();

        // Print the schema in a tree format
        df.printSchema();

        // Select everybody, but increment the age by 1
        df.select(col("name"), col("age").plus(1)).show();

        // Select people older than 21
        df.filter(col("age").gt(21)).show();

        // Count people by age
        df.groupBy("age").count().show();

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();



    }
}
