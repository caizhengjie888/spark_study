package com.kfk.spark.sql;

import com.kfk.spark.common.Comm;
import com.kfk.spark.common.CommSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/3
 * @time : 9:32 下午
 */
public class RDDToDFProgramJava {
    public static void main(String[] args) {

        SparkSession spark = CommSparkSession.getSparkSession();

        /**
         * 数据源
         * Michael, 29
         * Andy, 30
         * Justin, 19
         */
        String filePath = Comm.fileDirPath + "people.txt";

        List<StructField> fields = new ArrayList<StructField>();

        StructField structField_Name = DataTypes.createStructField("name",DataTypes.StringType,true);
        StructField structField_Age = DataTypes.createStructField("age",DataTypes.StringType,true);

        fields.add(structField_Name);
        fields.add(structField_Age);

        // 构造Schema
        StructType scheme = DataTypes.createStructType(fields);

        // 将rdd(people)转换成RDD[Row]
        JavaRDD<Row> personRdd = spark.read().textFile(filePath).javaRDD().map(line ->{
            String[] attributes = line.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        // 通过rdd和scheme创建DataFrame
        Dataset<Row> personDataFrame = spark.createDataFrame(personRdd,scheme);

        personDataFrame.show();

        /**
         * +-------+---+
         * |   name|age|
         * +-------+---+
         * |Michael| 29|
         * |   Andy| 30|
         * | Justin| 19|
         * +-------+---+
         */

        // 创建临时表
        personDataFrame.createOrReplaceTempView("person");

        Dataset<Row> resultDataFrame = spark.sql("select * from person a where a.age > 20");

        resultDataFrame.show();

        /**
         * +-------+---+
         * |   name|age|
         * +-------+---+
         * |Michael| 29|
         * |   Andy| 30|
         * +-------+---+
         */

    }
}
