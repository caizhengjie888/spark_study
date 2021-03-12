package com.kfk.spark.blacklist_project;

import com.kfk.spark.common.CommStreamingContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/16
 * @time : 8:21 下午
 */
public class BlackListJava {
    public static void main(String[] args) throws InterruptedException {
        JavaStreamingContext jssc = CommStreamingContext.getJssc();

        /**
         * 模拟创建黑名单
         * 数据格式：<String,Boolean> -> <"jack",true>
         */
        List<Tuple2<String,Boolean>> blacklist = new ArrayList<Tuple2<String,Boolean>>();
        blacklist.add(new Tuple2<>("jack",true));
        blacklist.add(new Tuple2<>("lili",true));

        JavaPairRDD<String,Boolean> blacklistRdd = jssc.sparkContext().parallelizePairs(blacklist);

        // nc数据源
        JavaReceiverInputDStream<String> userDstream = jssc.socketTextStream("bigdata-pro-m04",9999);

        /**
         * 源数据：2019-09-09 张三    -> 转换后的数据：  <"张三",2019-09-09 张三>
         */
        JavaPairDStream<String, String> userPair = userDstream.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                return new Tuple2<String, String>(line.split(" ")[1],line);
            }
        });

        /**
         * <"张三",2019-09-09 张三> <"张三",true> -> leftOuterJoin -> <”张三”,<张三 2019-09-09,true>> -> filter 过滤掉==true的数据
         */
        JavaDStream<String> stream = userPair.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> pair) throws Exception {

                // leftOuterJoin
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinrdd = pair.leftOuterJoin(blacklistRdd);

                // filter
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterjoinrdd =joinrdd.filter(
                        new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {

                        if (tuple._2._2.isPresent() && tuple._2._2.get()){
                            return false;
                        }
                        return true;
                    }
                });

                // map
                JavaRDD<String> rdd = filterjoinrdd.map(new Function<Tuple2<String,
                        Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        return tuple._1 + " : " + tuple._2._1 + " : " + tuple._2._2;
                    }
                });

                return rdd;
            }
        });

        stream.print();

        jssc.start();
        jssc.awaitTermination();

    }
}

