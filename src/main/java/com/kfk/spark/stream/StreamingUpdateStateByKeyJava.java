package com.kfk.spark.stream;

import com.kfk.spark.common.CommStreamingContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/15
 * @time : 10:08 下午
 */
public class StreamingUpdateStateByKeyJava {
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

        // lambda表达式写法
//        JavaPairDStream<String,Integer> wordcount = pair.updateStateByKey((values, state) -> {
//            Integer newValues = 0;
//            if (state.isPresent()){
//                newValues = state.get();
//            }
//
//            for (Integer value : values){
//                newValues += value;
//            }
//
//            return Optional.of(newValues);
//
//        });

        wordcount.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
