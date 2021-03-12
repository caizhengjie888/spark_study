package com.kfk.spark.core.project2;

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
 * @date : 2020/12/1
 * @time : 12:48 下午
 */
public class LogApp {

    public static JavaPairRDD<String,LogInfo> mapToPairValues(JavaRDD<String> rdd){

        JavaPairRDD<String,LogInfo> mapToPairRdd = rdd.mapToPair(new PairFunction<String, String, LogInfo>() {
            public Tuple2<String, LogInfo> call(String line) throws Exception {
                String diviceId = line.split("\t")[1];
                long timeStamp = Long.parseLong(line.split("\t")[0]);
                long upTraffic = Long.parseLong(line.split("\t")[2]);
                long downTraffic = Long.parseLong(line.split("\t")[3]);

                LogInfo logInfo = new LogInfo(timeStamp,upTraffic,downTraffic);
                return new Tuple2<String, LogInfo>(diviceId,logInfo);
            }
        });
        return mapToPairRdd;
    }

    public static JavaPairRDD<String,LogInfo> reduceByKeyValues(JavaPairRDD<String,LogInfo> mapToPairRdd){

        JavaPairRDD<String,LogInfo> reduceByKeyRdd = mapToPairRdd.reduceByKey(new Function2<LogInfo, LogInfo, LogInfo>() {
            public LogInfo call(LogInfo v1, LogInfo v2) throws Exception {

                long timeStamp = Math.min(v1.getTimeStamp(), v2.getTimeStamp());
                long upTraffic = v1.getUpTraffic() + v2.getUpTraffic();
                long downTraffic = v1.getDownTraffic() + v2.getDownTraffic();

                LogInfo logInfo = new LogInfo(timeStamp,upTraffic,downTraffic);

                return logInfo;
            }
        });

        return reduceByKeyRdd;
    }

    public static JavaPairRDD<LogSort,String> mapToPairSortValues(JavaPairRDD<String,LogInfo> reduceByKeyRdd){

        JavaPairRDD<LogSort,String> mapToPairSortRdd = reduceByKeyRdd.mapToPair(new PairFunction<Tuple2<String, LogInfo>, LogSort, String>() {
            public Tuple2<LogSort, String> call(Tuple2<String, LogInfo> stringLogInfoTuple2) throws Exception {

                long timeStamp = stringLogInfoTuple2._2.getTimeStamp();
                long upTraffic = stringLogInfoTuple2._2.getUpTraffic();
                long downTraffic = stringLogInfoTuple2._2.getDownTraffic();

                LogSort logSort = new LogSort(timeStamp,upTraffic,downTraffic);
                return new Tuple2<LogSort, String>(logSort,stringLogInfoTuple2._1);
            }
        });
        return mapToPairSortRdd;
    }

    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();
        JavaRDD<String> rdd = sc.textFile("/Users/caizhengjie/IdeaProjects/spark_study01/src/main/java/com/kfk/spark/datas/access.log");

        // rdd map() -> <diviceId,LogInfo(timeStamp,upTraffic,downTraffic)>
        JavaPairRDD<String,LogInfo> mapToPairRdd = mapToPairValues(rdd);

        // mapToPairRdd reduceByKey() -> <diviceId,LogInfo(timeStamp,upTraffic,downTraffic)>
        JavaPairRDD<String,LogInfo> reduceByKeyRdd = reduceByKeyValues(mapToPairRdd);

        // reduceByKeyRdd map() -> <LogSort(timeStamp,upTraffic,downTraffic),diviceId>
        JavaPairRDD<LogSort,String> mapToPairSortRdd = mapToPairSortValues(reduceByKeyRdd);

        // sortByKey()
        JavaPairRDD<LogSort,String> sortByKeyRdd = mapToPairSortRdd.sortByKey(false);

        //take(10)
        List<Tuple2<LogSort, String>> takeValues = sortByKeyRdd.take(10);

        for (Tuple2<LogSort, String> sortStringTuple2 :takeValues){
            System.out.println(sortStringTuple2._2 + " : " + sortStringTuple2._1.getUpTraffic() + " : " + sortStringTuple2._1.getDownTraffic());

        }
    }
}
