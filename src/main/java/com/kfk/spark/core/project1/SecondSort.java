package com.kfk.spark.core.project1;

import com.kfk.spark.common.CommSparkContext;
import com.kfk.spark.core.project1.SecondSortKey;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/1
 * @time : 11:20 上午
 */
public class SecondSort {
    public static void main(String[] args) {
        final JavaSparkContext sc = CommSparkContext.getsc();
        List list = Arrays.asList("class1 90","class2 93","class1 97",
                "class1 89","class3 99","class3 78",
                "class1 79","class2 85","class2 89",
                "class2 96","class3 92","class3 86");

        JavaRDD rdd = sc.parallelize(list);

        // rdd map() -> <SecondSortKey(className,score),string>

        JavaPairRDD<SecondSortKey,String> mapToPairRdd = rdd.mapToPair(new PairFunction<String,SecondSortKey,String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(String line) throws Exception {
                String className = line.split(" ")[0];
                int score = Integer.parseInt(line.split(" ")[1]);

                SecondSortKey secondSortKey = new SecondSortKey(className,score);
                return new Tuple2<SecondSortKey, String>(secondSortKey,line);
            }
        });

        // sortByKey
        JavaPairRDD<SecondSortKey,String> sortByKeyValues = mapToPairRdd.sortByKey(false);

        sortByKeyValues.foreach(new VoidFunction<Tuple2<SecondSortKey, String>>() {
            @Override
            public void call(Tuple2<SecondSortKey, String> secondSortKeyStringTuple2) throws Exception {
                System.out.println(secondSortKeyStringTuple2._2);
            }
        });
    }
}
