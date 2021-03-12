package com.kfk.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/25
 * @time : 7:10 下午
 */
public class TransformationJava {

    public static JavaSparkContext getsc(){
        SparkConf sparkConf = new SparkConf().setAppName("TransformationJava").setMaster("local");
        return new JavaSparkContext(sparkConf);
    }

    public static void main(String[] args) {

         map();
         filter();
         flatmap();
         groupByKey();
         reduceByKey();
         sortByKey();
         join();
         cogroup();

    }

    /**
     * 数据集一：(2,"lili")                cogroup() -> <2,<"lili",(90,95,99)>>
     * 数据集二：(2,90)(2,95)(2,99)
     */
    private static void cogroup() {
        List stuList = Arrays.asList(new Tuple2<Integer,String>(1,"alex"),
                new Tuple2<Integer,String>(2,"lili"),
                new Tuple2<Integer,String>(3,"cherry"),
                new Tuple2<Integer,String>(4,"jack"),
                new Tuple2<Integer,String>(5,"jone"),
                new Tuple2<Integer,String>(6,"lucy"),
                new Tuple2<Integer,String>(7,"aliy"));

        List scoreList = Arrays.asList(new Tuple2<Integer,Integer>(1,90),
                new Tuple2<Integer,Integer>(2,79),
                new Tuple2<Integer,Integer>(2,95),
                new Tuple2<Integer,Integer>(2,99),
                new Tuple2<Integer,Integer>(3,87),
                new Tuple2<Integer,Integer>(3,88),
                new Tuple2<Integer,Integer>(3,89),
                new Tuple2<Integer,Integer>(4,98),
                new Tuple2<Integer,Integer>(5,89),
                new Tuple2<Integer,Integer>(6,93),
                new Tuple2<Integer,Integer>(7,96));

        JavaSparkContext sc = getsc();

        JavaPairRDD javaPairStuRdd = sc.parallelizePairs(stuList);
        JavaPairRDD javaPairScoreRdd = sc.parallelizePairs(scoreList);

        JavaPairRDD<Integer,Tuple2<Iterable<Integer>,Iterable<Integer>>> cogroupValues = javaPairStuRdd.cogroup(javaPairScoreRdd);
        cogroupValues.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2._1);
                System.out.println(integerTuple2Tuple2._2._1 + " : " + integerTuple2Tuple2._2._2);
            }
        });
    }

    /**
     * 数据集一：(1,"alex")   join() -> <1,<"alex",90>>
     * 数据集二：(1,90)
     */
    private static void join() {

        List stuList = Arrays.asList(new Tuple2<Integer,String>(1,"alex"),
                new Tuple2<Integer,String>(2,"lili"),
                new Tuple2<Integer,String>(3,"cherry"),
                new Tuple2<Integer,String>(4,"jack"),
                new Tuple2<Integer,String>(5,"jone"),
                new Tuple2<Integer,String>(6,"lucy"),
                new Tuple2<Integer,String>(7,"aliy"));

        List scoreList = Arrays.asList(new Tuple2<Integer,Integer>(1,90),
                new Tuple2<Integer,Integer>(2,95),
                new Tuple2<Integer,Integer>(3,87),
                new Tuple2<Integer,Integer>(4,98),
                new Tuple2<Integer,Integer>(5,89),
                new Tuple2<Integer,Integer>(6,93),
                new Tuple2<Integer,Integer>(7,96));

        JavaSparkContext sc = getsc();

        JavaPairRDD javaPairStuRdd = sc.parallelizePairs(stuList);
        JavaPairRDD javaPairScoreRdd = sc.parallelizePairs(scoreList);

        JavaPairRDD<Integer,Tuple2<String,Integer>> joinValue = javaPairStuRdd.join(javaPairScoreRdd);

        joinValue.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2._1);
                System.out.println(integerTuple2Tuple2._2._1 + " : " + integerTuple2Tuple2._2._2);
            }
        });
    }

    /**
     * <90,alex>
     * <95,lili>    sortByKey() -> <87,cherry> <90,alex> <95,lili>
     * <87,cherry>
     */
    private static void sortByKey() {

        List list = Arrays.asList(new Tuple2<Integer,String>(90,"alex"),
                new Tuple2<Integer,String>(95,"lili"),
                new Tuple2<Integer,String>(87,"cherry"),
                new Tuple2<Integer,String>(98,"jack"),
                new Tuple2<Integer,String>(89,"jone"),
                new Tuple2<Integer,String>(93,"lucy"),
                new Tuple2<Integer,String>(96,"aliy"));

        JavaPairRDD<Integer, String> javaPairRdd = getsc().parallelizePairs(list);
        JavaPairRDD<Integer,String> sortByKeyValues = javaPairRdd.sortByKey(true);

        sortByKeyValues.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2._1 + " : " + integerStringTuple2._2);
            }
        });
    }

    /**
     * <class_1,(90,87,98,96)>  reduceByKey()  -> <class_1,(90+87+98+96)>
     * <class_2,(95,89,93)>     reduceByKey()  -> <class_2,(95+89+93)>
     */
    private static void reduceByKey() {

        List list = Arrays.asList(new Tuple2<String,Integer>("class_1",90),
                new Tuple2<String,Integer>("class_2",95),
                new Tuple2<String,Integer>("class_1",87),
                new Tuple2<String,Integer>("class_1",98),
                new Tuple2<String,Integer>("class_2",89),
                new Tuple2<String,Integer>("class_2",93),
                new Tuple2<String,Integer>("class_1",96));

        final JavaPairRDD<String, Integer> javaPairRdd = getsc().parallelizePairs(list);

        JavaPairRDD<String,Integer> reduceByKeyValues = javaPairRdd.reduceByKey(new Function2<Integer,Integer,Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        reduceByKeyValues.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + " : " + stringIntegerTuple2._2);
            }
        });
    }

    /**
     * class_1 90       groupByKey()  -> <class_1,(90,87,98,96)>  <class_2,(95,89,93)>
     * class_2 95
     * class_1 87
     * class_1 98
     * class_2 89
     * class_2 93
     * class_1 96
     */
    private static void groupByKey() {

        List list = Arrays.asList(new Tuple2<String,Integer>("class_1",90),
                new Tuple2<String, Integer>("class_2",95),
                new Tuple2<String,Integer>("class_1",87),
                new Tuple2<String,Integer>("class_1",98),
                new Tuple2<String,Integer>("class_2",89),
                new Tuple2<String,Integer>("class_2",93),
                new Tuple2<String,Integer>("class_1",96));

        JavaPairRDD<String,Integer> javaPairRdd = getsc().parallelizePairs(list);

        JavaPairRDD<String,Iterable<Integer>> groupByKeyValue = javaPairRdd.groupByKey();

        groupByKeyValue.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String,Iterable<Integer>> stringIteratorTuple2) throws Exception {
                System.out.println(stringIteratorTuple2._1);

                Iterator<Integer> iterator = stringIteratorTuple2._2.iterator();
                while (iterator.hasNext()){
                    System.out.println(iterator.next());
                }
            }
        });
    }

    /**
     * hbase hadoop hive
     * java python          flatmap() -> hbase hadoop hive java python java python
     * java python
     */
    private static void flatmap() {

        List<String> list = Arrays.asList("hbase hadoop hive","java python","java python");

        JavaRDD<String> javaRdd = getsc().parallelize(list);

        JavaRDD<String> flatMapValue = javaRdd.flatMap(new FlatMapFunction<String,String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        flatMapValue.foreach(new VoidFunction<String>() {
            @Override
            public void call(String value) throws Exception {
                System.out.println(value);
            }
        });
    }

    /**
     * 1,2,3,4,5,6,7,8,9,10     filter() -> 2,4,6,8,10
     */
    private static void filter() {

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> javaRdd = getsc().parallelize(list);

        // 取偶数
        JavaRDD<Integer> filterValue = javaRdd.filter(new Function<Integer,Boolean>() {
            @Override
            public Boolean call(Integer value) throws Exception {
                return value % 2 == 0;
            }
        });

        filterValue.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer o) throws Exception {
                System.out.println(o);
            }
        });
    }

    /**
     * 1,2,3,4,5    map() -> 10,20,30,40,50
     */
    public static void map(){

        List<Integer> list = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> javaRdd = getsc().parallelize(list);

        JavaRDD<Integer> mapValue = javaRdd.map(new Function<Integer,Integer>() {
            @Override
            public Integer call(Integer value) throws Exception {
                return value * 10;
            }
        });

        mapValue.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer o) throws Exception {
                System.out.println(o);
            }
        });
    }
}
