package com.kfk.spark.sql;

import com.kfk.spark.common.Comm;
import com.kfk.spark.common.CommSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/2
 * @time : 10:07 下午
 */
public class RDDToDFReflectionJava {
    public static void main(String[] args) {
        SparkSession spark = CommSparkSession.getSparkSession();

        /**
         * 数据源
         * Michael, 29
         * Andy, 30
         * Justin, 19
         */
        String filePath = Comm.fileDirPath + "people.txt";

        // 将文件转换成rdd
        JavaRDD<Person> personrdd = spark.read().textFile(filePath).javaRDD().map(line -> {

            String name = line.split(",")[0];
            long age = Long.parseLong(line.split(",")[1].trim());
            Person person = new Person(name,age);

            return person;

        });

        // 通过反射将rdd转换成DataFrame
        Dataset<Row> personDf = spark.createDataFrame(personrdd,Person.class);
        personDf.show();

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
        personDf.createOrReplaceTempView("person");

        Dataset<Row> resultDf = spark.sql("select * from person a where a.age > 20");
        resultDf.show();

        /**
         * +-------+---+
         * |   name|age|
         * +-------+---+
         * |Michael| 29|
         * |   Andy| 30|
         * +-------+---+
         */

        // 将DataFrame转换成rdd
        JavaRDD<Person> resultRdd = resultDf.javaRDD().map(line -> {

            Person person = new Person();
            person.setName(line.getAs("name"));
            person.setAge(line.getAs("age"));

            return person;

        });

        for (Person person : resultRdd.collect()){
            System.out.println(person.getName() + " : " + person.getAge());
        }

        /**
         * Michael : 29
         * Andy : 30
         */
    }
}
