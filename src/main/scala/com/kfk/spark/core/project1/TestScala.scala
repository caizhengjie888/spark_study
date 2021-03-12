package com.kfk.spark.core.project1

import com.kfk.spark.common.CommSparkContextScala

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/5
 * @time : 11:35 下午
 */
object TestScala {
    def main(args: Array[String]): Unit = {

        val sc = CommSparkContextScala.getsc();

        //12 宋江 25 男 chinese 50
        //12 宋江 25 男 math 60
        //12 宋江 25 男 english 70
        //12 吴用 20 男 chinese 50
        //12 吴用 20 男 math 50
        //12 吴用 20 男 english 50
        //12 杨春 19 女 chinese 70
        //12 杨春 19 女 math 70
        //12 杨春 19 女 english 70
        //13 李逵 25 男 chinese 60
        //13 李逵 25 男 math 60
        //13 李逵 25 男 english 70
        //13 林冲 20 男 chinese 50
        //13 林冲 20 男 math 60
        //13 林冲 20 男 english 50
        //13 王英 19 女 chinese 70
        //13 王英 19 女 math 80
        //13 王英 19 女 english 70

        // 1.读取文件的数据test.txt
        val rdd = sc.textFile("/Users/caizhengjie/Desktop/data.txt")

        // 2.一共有多少个小于20岁的人参加考试？2
        val count1 = rdd.map(line => line.split(" ")).filter(x => x(2).toInt < 20).map(y => (y(1),y)).groupByKey().count()
        println(count1)

        // 3.一共有多少个等于20岁的人参加考试？2
        val count2 = rdd.map(line => line.split(" ")).filter(x => x(2).toInt.equals(20)).map(y => (y(1),y)).groupByKey().count()
        println(count2)

        // 4.一共有多少个大于20岁的人参加考试？2
        val count3 = rdd.map(line => line.split(" ")).filter(x => x(2).toInt > 20).map(y => (y(1),y)).groupByKey().count()
        println(count3)

        // 5.一共有多个男生参加考试？12
        val count4 = rdd.map(line => line.split(" ")).filter(x => x(3).equals("男")).count()
        println(count4)

        // 6.一共有多少个女生参加考试？6
        val count5 = rdd.map(line => line.split(" ")).filter(x => x(3).equals("女")).count()
        println(count5)

        // 7.12班有多少人参加考试？9
        val count6 = rdd.map(line => line.split(" ")).filter(x => x(0).equals("12")).count()
        println(count6)

        // 8.13班有多少人参加考试？9
        val count7 = rdd.map(line => line.split(" ")).filter(x => x(0).equals("13")).count()
        println(count7)

        // 9.语文科目的平均成绩是多少？58.333333333333336
        val score1 = rdd.map(line => line.split(" ")).filter(x => x(4).equals("chinese")).map(x => x(5).toInt).mean()
        println(score1)

        // 10.数学科目的平均成绩是多少？63.333333333333336
        val score2 = rdd.map(line => line.split(" ")).filter(x => x(4).equals("math")).map(x => x(5).toInt).mean()
        println(score2)

        // 11.英语科目的平均成绩是多少？63.333333333333336
        val score3 = rdd.map(line => line.split(" ")).filter(x => x(4).equals("english")).map(x => x(5).toInt).mean()
        println(score3)

        // 12.每个人平均成绩是多少？
        // 王英 : 73
        // 杨春 : 70
        // 宋江 : 60
        // 李逵 : 63
        // 吴用 : 50
        // 林冲 : 53
        val score4 = rdd.map(line => line.split(" ")).map(y => (y(1),y(5).toInt)).groupByKey().map(x => (x._1,(x._2.sum)/x._2.size))
        score4.foreach(x => println(x._1 + " : " + x._2))

        // 13.12班平均成绩是多少？60.0
        val score5 = rdd.map(line => line.split(" ")).filter(x => x(0).equals("12")).map(x => x(5).toInt).mean()
        println(score5)

        // 14.12班男生平均总成绩是多少？165.0
        val score6 = rdd.map(line => line.split(" ")).filter(x => x(0).equals("12")).filter(x => x(3).equals("男")).map(y => (y(1),y(5).toInt)).reduceByKey(_ + _).map(x => x._2).mean()
        println(score6)

        // 15.12班女生平均总成绩是多少？210.0
        val score7 = rdd.map(line => line.split(" ")).filter(x => x(0).equals("12")).filter(x => x(3).equals("女")).map(y => (y(1),y(5).toInt)).reduceByKey(_ + _).map(x => x._2).mean()
        println(score7)

        // 16.13班平均成绩是多少？63.333333333333336
        val score8 = rdd.map(line => line.split(" ")).filter(x => x(0).equals("13")).map(x => x(5).toInt).mean()
        println(score8)

        // 17.13班男生平均总成绩是多少？175.0
        val score9 = rdd.map(line => line.split(" ")).filter(x => x(0).equals("13")).filter(x => x(3).equals("男")).map(y => (y(1),y(5).toInt)).reduceByKey(_ + _).map(x => x._2).mean()
        println(score9)

        // 18.13班女生平均总成绩是多少？220.0
        val score10 = rdd.map(line => line.split(" ")).filter(x => x(0).equals("13")).filter(x => x(3).equals("女")).map(y => (y(1),y(5).toInt)).reduceByKey(_ + _).map(x => x._2).mean()
        println(score10)

        // 19.全校语文成绩最高分是多少？70
        val score11 = rdd.map(line => line.split(" ")).filter(x => x(4).equals("chinese")).map(x => x(5).toInt).max()
        println(score11)

        // 20.12班语文成绩最低分是多少？50
        val score12 = rdd.map(line => line.split(" ")).filter(x => x(0).equals("12")).filter(x => x(4).equals("chinese")).map(x => x(5).toInt).min()
        println(score12)

        // 21.13班数学最高成绩是多少？80
        val score13 = rdd.map(line => line.split(" ")).filter(x => x(0).equals("13")).filter(x => x(4).equals("math")).map(x => x(5).toInt).max()
        println(score13)

        // 22.总成绩大于150分的12班的女生有几个？1
        val count8 = rdd.map(line => line.split(" ")).filter(x => x(0).equals("12")).filter(x => x(3).equals("女")).map(y => (y(1),y(5).toInt)).reduceByKey(_ + _).map(x => x._2 >150).count()
        println(count8)

        // 23. 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？

    }

}
