package com.kfk.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/26
 * @time : 10:42 下午
 */
public class ActionJava {

    public static JavaSparkContext getsc(){
        SparkConf sparkConf = new SparkConf().setAppName("ActionJava").setMaster("local");
        return new JavaSparkContext(sparkConf);
    }

    public static void main(String[] args) {

        reduce();
        collect();
        count();
        take();
        save();
        countByKey();
    }

    /**
     * <"class_1","alex">
     * <"class_2","jone">
     * <"class_1","lucy">                           <class_1,4>
     * <"class_1","lili">       countByKey() ->
     * <"class_2","ben">                            <class_2,3>
     * <"class_2","jack">
     * <"class_1","cherry">
     */
    private static void countByKey() {

        List list = Arrays.asList(new Tuple2<String,String>("class_1","alex"),
                new Tuple2<String,String>("class_2","jone"),
                new Tuple2<String,String>("class_1","lucy"),
                new Tuple2<String,String>("class_1","lili"),
                new Tuple2<String,String>("class_2","ben"),
                new Tuple2<String,String>("class_2","jack"),
                new Tuple2<String,String>("class_1","cherry"));

        JavaPairRDD javaPairRdd = getsc().parallelizePairs(list);

        Map<String,Integer> countByKeyValues = javaPairRdd.countByKey();

        for (Map.Entry obj : countByKeyValues.entrySet()){
            System.out.println(obj.getKey() + " : " + obj.getValue());
        }
    }

    /**
     * saveAsTextFile()
     */
    private static void save() {

        List<Integer> list = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> javaRdd = getsc().parallelize(list);

        javaRdd.saveAsTextFile("hdfs://bigdata-pro-m04:9000/user/caizhengjie/datas/javaRdd");

    }

    /**
     * 1,2,3,4,5    take(3) -> [1,2,3]
     */
    private static void take() {

        List<Integer> list = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> javaRdd = getsc().parallelize(list);

        List<Integer> listValues = javaRdd.take(3);

        for (int value : listValues){
            System.out.println(value);
        }
    }

    /**
     * 1,2,3,4,5    count() -> 5
     */
    private static void count() {

        List<Integer> list = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> javaRdd = getsc().parallelize(list);

        System.out.println(javaRdd.count());
    }

    /**
     * 1,2,3,4,5    collect() -> [1,2,3,4,5]
     */
    private static void collect() {

        List<Integer> list = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> javaRdd = getsc().parallelize(list);

        List<Integer> collectValues = javaRdd.collect();
        for (int value : collectValues){
            System.out.println(value);
        }
    }

    /**
     * 1,2,3,4,5    reduce() -> 15
     */
    private static void reduce() {

        List<Integer> list = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> javaRdd = getsc().parallelize(list);

        Integer reduceValues = javaRdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        System.out.println(reduceValues);
    }
}
