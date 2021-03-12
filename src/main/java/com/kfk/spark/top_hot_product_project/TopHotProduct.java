package com.kfk.spark.top_hot_product_project;

import com.kfk.spark.common.CommSparkSession;
import com.kfk.spark.common.CommStreamingContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/19
 * @time : 1:36 下午
 */
public class TopHotProduct {
    public static void main(String[] args) throws InterruptedException {
        JavaStreamingContext jssc = CommStreamingContext.getJssc();

        /**
         * 输入数据源模型
         * --------------------------
         * 姓名		商品分类		商品名称
         * alex 	bigdata		hadoop
         * alex 	bigdata		spark
         * jack 	bigdata		flink
         * alex 	bigdata		hadoop
         * alex 	language	java
         * pony 	language	python
         * jone 	language	java
         * lili 	bigdata		hadoop
         * --------------------------
         */
        JavaReceiverInputDStream<String> inputDstream = jssc.socketTextStream("bigdata-pro-m04",9999);

        /**
         * mapToPair
         * <bigdata_hadoop,1>
         * <bigdata_spark,1>
         * <bigdata_flink,1>
         */
        JavaPairDStream<String, Integer> pairDstream = inputDstream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String lines) throws Exception {

                String[] line = lines.split(" ");
                return new Tuple2<>(line[1] + "_" + line[2], 1);
            }
        });

        /**
         * reduceByKeyAndWindow
         * <bigdata_hadoop,5>
         * <bigdata_spark,2>
         * <bigdata_flink,4>
         */
        JavaPairDStream<String, Integer> windowPairStream = pairDstream.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }, Durations.seconds(60),Durations.seconds(10));

        /**
         * foreachRDD
         */
        windowPairStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRdd) throws Exception {

                // 转换成RDD[Row]
                JavaRDD<Row> countRowRdd = stringIntegerJavaPairRdd.map(new Function<Tuple2<String, Integer>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Integer> tuple) throws Exception {

                        String category = tuple._1.split("_")[0];
                        String product = tuple._1.split("_")[1];
                        Integer productCount = tuple._2;
                        return RowFactory.create(category,product,productCount);
                    }
                });

                List<StructField> structFields = new ArrayList<StructField>();
                structFields.add(DataTypes.createStructField("category",DataTypes.StringType,true));
                structFields.add(DataTypes.createStructField("product",DataTypes.StringType,true));
                structFields.add(DataTypes.createStructField("productCount",DataTypes.IntegerType,true));

                // 构造Schema
                StructType structType = DataTypes.createStructType(structFields);

                // 创建SparkSession
                SparkSession spark = CommSparkSession.getSparkSession();

                // 通过rdd和scheme创建DataFrame
                Dataset<Row> df = spark.createDataFrame(countRowRdd,structType);

                // 创建临时表
                df.createOrReplaceTempView("product_click");

                /**
                 * 统计每个种类下点击次数排名前3名的商品
                 */
                spark.sql("select category,product,productCount from product_click " +
                        "order by productCount desc limit 3").show();

                /**
                 * +--------+-------+------------+
                 * |category|product|productCount|
                 * +--------+-------+------------+
                 * | bigdata| hadoop|           4|
                 * | bigdata|  kafka|           2|
                 * | bigdata|  flink|           2|
                 * +--------+-------+------------+
                 */
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
