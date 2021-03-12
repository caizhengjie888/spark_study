package com.kfk.spark.foreachrdd_project;

import com.kfk.spark.common.CommStreamingContext;
import com.kfk.spark.common.ConnectionPool;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/18
 * @time : 7:49 下午
 */
public class ForeachPersistMySQL {
    public static void main(String[] args) throws InterruptedException {
        JavaStreamingContext jssc = CommStreamingContext.getJssc();

        // 要使用UpdateStateByKey算子就必须设置一个Checkpoint目录，开启Checkpoint机制
        // 以便于内存数据丢失时，可以从Checkpoint中恢复数据
        jssc.checkpoint("hdfs://bigdata-pro-m04:9000/user/caizhengjie/datas/sparkCheckpoint");

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("bigdata-pro-m04",9999);

        // flatmap
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // map
        JavaPairDStream<String,Integer> pair =  words.mapToPair(word -> new Tuple2<>(word,1));

        // 通过spark来维护一份每个单词的全局统计次数
        JavaPairDStream<String,Integer> wordcount = pair.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {

                Integer newValues = 0;
                if (state.isPresent()){
                    newValues = state.get();
                }

                for (Integer value : values){
                    newValues += value;
                }

                return Optional.of(newValues);
            }
        });

        // foreachRDD
        wordcount.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRdd) throws Exception {
                stringIntegerJavaPairRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                        Tuple2<String, Integer> wordcount = null;
                        Connection conn = ConnectionPool.getConnection();
                        while (tuple2Iterator.hasNext()){
                            wordcount = tuple2Iterator.next();

                            String sql = "insert into spark.wordcount(word,count) values('"+wordcount._1+"', '"+wordcount._2+"')";

                            Statement statement = conn.createStatement();
                            statement.executeUpdate(sql);
                        }
                        ConnectionPool.returnConnection(conn);
                    }
                });

            }
        });

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
