package com.kfk.spark.traffic_analysis_project;

import com.kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/30
 * @time : 6:36 下午
 */
public class LogApp {

    /**
     * rdd映射成key-value方式<diviceId,LogInfo>
     * rdd map() -> <diviceId,LogInfo(timeStamp,upTraffic,downTraffic)>
     * @param rdd
     * @return
     */
    public static JavaPairRDD<String,LogInfo> mapToPairValues(JavaRDD<String> rdd){

        JavaPairRDD<String,LogInfo> mapToPairRdd =  rdd.mapToPair(new PairFunction<String, String, LogInfo>() {
            @Override
            public Tuple2<String, LogInfo> call(String line) throws Exception {

                long timeStamp = Long.parseLong(line.split("\t")[0]);
                String diviceId = String.valueOf(line.split("\t")[1]);
                long upTraffic = Long.parseLong(line.split("\t")[2]);
                long downTraffic = Long.parseLong(line.split("\t")[3]);

                LogInfo logInfo = new LogInfo(timeStamp,upTraffic,downTraffic);

                return new Tuple2<String, LogInfo>(diviceId,logInfo);
            }
        });
        return mapToPairRdd;
    }

    /**
     * 根据diviceId进行聚合
     * mapToPairRdd reduceByKey() -> <diviceId,LogInfo(timeStamp,upTraffic,downTraffic)>
     * @param mapPairRdd
     * @return
     */
    public static JavaPairRDD<String,LogInfo> reduceByKeyValues(JavaPairRDD<String,LogInfo> mapPairRdd){

        JavaPairRDD<String,LogInfo> reduceByKeyRdd = mapPairRdd.reduceByKey(new Function2<LogInfo, LogInfo, LogInfo>() {
            @Override
            public LogInfo call(LogInfo v1, LogInfo v2) throws Exception {
                long timeStamp = Math.min(v1.getTimeStamp(), v2.getTimeStamp());
                long upTraffic = v1.getUpTraffic() + v2.getUpTraffic();
                long downTraffic = v1.getDownTraffic() + v2.getDownTraffic();

                LogInfo logInfo = new LogInfo();
                logInfo.setTimeStame(timeStamp);
                logInfo.setUpTraffic(upTraffic);
                logInfo.setDownTraffic(downTraffic);
                return logInfo;
            }
        });
        return reduceByKeyRdd;
    }

    /**
     * reduceByKeyRdd map() -> <LogSort(timeStamp,upTraffic,downTraffic),diviceId>
     * @param aggregateByKeyRdd
     * @return
     */
    public static JavaPairRDD<LogSort,String> mapToPairSortValues(JavaPairRDD<String,LogInfo> aggregateByKeyRdd){
        JavaPairRDD<LogSort,String> mapToPairSortRdd = aggregateByKeyRdd.mapToPair(new PairFunction<Tuple2<String, LogInfo>, LogSort, String>() {
            @Override
            public Tuple2<LogSort, String> call(Tuple2<String, LogInfo> stringLogInfoTuple2) throws Exception {

                String diviceId = stringLogInfoTuple2._1;
                long timeStamp = stringLogInfoTuple2._2.getTimeStamp();
                long upTraffic = stringLogInfoTuple2._2.getUpTraffic();
                long downTraffic = stringLogInfoTuple2._2.getDownTraffic();

                LogSort logSort = new LogSort(timeStamp,upTraffic,downTraffic);

                return new Tuple2<LogSort, String>(logSort,diviceId);
            }
        });
        return mapToPairSortRdd;
    }

    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        JavaRDD<String> rdd = sc.textFile("/Users/caizhengjie/IdeaProjects/spark_study01/src/main/resources/datas/access.log");

        // rdd map() -> <diviceId,LogInfo(timeStamp,upTraffic,downTraffic)>
        JavaPairRDD<String,LogInfo> mapToPairRdd = mapToPairValues(rdd);

        // mapToPairRdd reduceByKey() -> <diviceId,LogInfo(timeStamp,upTraffic,downTraffic)>
        JavaPairRDD<String,LogInfo> reduceByKeyRdd = reduceByKeyValues(mapToPairRdd);

        // reduceByKeyRdd map() -> <LogSort(timeStamp,upTraffic,downTraffic),diviceId>
        JavaPairRDD<LogSort, String> mapToPairSortRdd = mapToPairSortValues(reduceByKeyRdd);

        // sortByKey
        JavaPairRDD<LogSort,String> sortByKeyValues = mapToPairSortRdd.sortByKey(false);

        // TopN
        List<Tuple2<LogSort,String>> sortKeyList = sortByKeyValues.take(10);

        for (Tuple2<LogSort,String> logSortStringTuple2 : sortKeyList){
            System.out.println(logSortStringTuple2._2 + " : " + logSortStringTuple2._1.getUpTraffic() + " : " + logSortStringTuple2._1.getDownTraffic());
        }
    }
}
