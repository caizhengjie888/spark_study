package com.kfk.spark.web_analysis_project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.mutable.StringBuilder;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/12
 * @time : 9:32 下午
 */
public class WebLogsAnalysisSpark {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("WebLogsAnalysis")
                .master("local")
                .config("spark.sql.warehouse.dir", "/Users/caizhengjie/Document/spark/spark-warehouse")
                .enableHiveSupport()
                .getOrCreate();

        String yesterday = Tools.getYesterday();

        // 需求一：对网站的pv分析
        Dataset<Row> pagePV = calculatePagePV(yesterday,spark);

        // 需求二：对网站的uv分析
        Dataset<Row> pageUV = calculatePageUV(yesterday,spark);

        // 需求三：新用户注册比率
        double regisRate = calculateNewUserRegis(yesterday,spark);

        // 需求四：用户跳出率
        Dataset<Row> leaveRate = calculateUserLeaveRate(yesterday,spark);

        // 需求五：版块热度排行榜
        Dataset<Row> sectionSort = calculateSectionSort(yesterday,spark);

        // 将分析出的结果表写入到mysql数据库中
        writeData(sectionSort,"sectionSort_info");

    }

    /**
     * 计算每天每个页面PV并且对其排序
     * 排序的好处：排序后插入到mysql，Java web系统要查询每天pv top10的页面，直接查询mysql表limit10就可以
     * 如果不排序的话，那么Java web系统就要做排序，这样返回会影响Java web系统的性能，影响用户访问的时间
     * @param date
     * @param spark
     */
    public static Dataset<Row> calculatePagePV(String date, SparkSession spark){

        // 写法一
        String sql = "select cdate,pageid,pv_count from " +
                "(select cdate,pageid,count(pageid) pv_count from hivespark.news_access " +
                "where action = 'view' and cdate = '"+date+"' group by cdate,pageid) " +
                "news_table order by pv_count desc limit 10 ";

        // 写法二
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("select cdate,pageid,pv_count from ");
        stringBuilder.append(" ( ");
        stringBuilder.append(" select cdate,pageid,count(pageid) pv_count from hivespark.news_access ");
        stringBuilder.append(" where action = 'view' and cdate = '"+date+"' group by cdate,pageid");
        stringBuilder.append(" ) ");
        stringBuilder.append(" news_table order by pv_count desc limit 10 ");

        spark.sql(stringBuilder.toString()).show();

        return spark.sql(stringBuilder.toString());

        /**
         * +----------+------+--------+
         * |     cdate|pageid|pv_count|
         * +----------+------+--------+
         * |2020-12-12|   462|      23|
         * |2020-12-12|   719|      21|
         * |2020-12-12|   943|      20|
         * |2020-12-12|   616|      20|
         * |2020-12-12|   623|      20|
         * |2020-12-12|   517|      19|
         * |2020-12-12|     9|      19|
         * |2020-12-12|   649|      19|
         * |2020-12-12|   467|      18|
         * |2020-12-12|   671|      18|
         * +----------+------+--------+
         */

    }

    /**
     * 计算每天每个页面UV并且对其排序
     * 1、
     * @param date
     * @param spark
     */
    public static Dataset<Row> calculatePageUV(String date, SparkSession spark){

        // 写法一
        String sql = "select cdate,pageid,count(pageid) uv_count from " +
                "(select cdate,pageid,count(userid) count from hivespark.news_access " +
                "where action = 'view' and cdate = '"+date+"' group by cdate,pageid,userid) " +
                "t group by cdate,pageid order by uv_count desc limit 10";

        // 写法二
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("select cdate,pageid,count(pageid) uv_count from ");
        stringBuilder.append(" ( ");
        stringBuilder.append(" select cdate,pageid,count(userid) count from hivespark.news_access ");
        stringBuilder.append(" where action = 'view' and cdate = '"+date+"' group by cdate,pageid,userid ");
        stringBuilder.append(" ) ");
        stringBuilder.append(" t group by cdate,pageid order by uv_count desc limit 10 ");

        spark.sql(stringBuilder.toString()).show();

        return  spark.sql(stringBuilder.toString());

        /**
         * +----------+------+--------+
         * |     cdate|pageid|uv_count|
         * +----------+------+--------+
         * |2020-12-12|   462|      23|
         * |2020-12-12|   719|      19|
         * |2020-12-12|   623|      19|
         * |2020-12-12|   585|      18|
         * |2020-12-12|   467|      18|
         * |2020-12-12|     9|      18|
         * |2020-12-12|   517|      18|
         * |2020-12-12|   732|      18|
         * |2020-12-12|   437|      18|
         * |2020-12-12|   473|      17|
         * +----------+------+--------+
         */

    }

    /**
     * 计算每天新用户的注册比例
     *
     * 1、先获取昨天所有访问行为中新用户的访问总数，即where date = yesterday and userid = 'null'
     * 2、获取昨天总的注册用户数
     * 3、新用户的注册比例 = 当天注册用户数 / 当天新用户的访问总数
     * @param date
     * @param spark
     */
    public static double calculateNewUserRegis(String date, SparkSession spark){

        // 昨天所有访问行为中新用户的访问总数（写法一）
        String allNewViewSql = "select count(1) allNewView from hivespark.news_access where action = 'view' and cdate = '"+date+"' and userid = 'null' ";

        // 写法二
        StringBuilder allNewViewBuilder = new StringBuilder();
        allNewViewBuilder.append("select count(1) allNewView from hivespark.news_access ");
        allNewViewBuilder.append(" where action = 'view' and cdate = '"+date+"' and userid = 'null' ");

        spark.sql(allNewViewSql).show();

        /**
         * +----------+
         * |allNewView|
         * +----------+
         * |       977|
         * +----------+
         */

        // 昨天总的注册用户数

        String allNewRegisSql = "select count(1) allNewRegis from hivespark.news_access where action = 'register' and cdate = '"+date+"'";

        // 写法二
        StringBuilder allNewRegisBuilder = new StringBuilder();
        allNewRegisBuilder.append("select count(1) allNewRegis from hivespark.news_access ");
        allNewRegisBuilder.append(" where action = 'register' and cdate = '"+date+"' ");

        spark.sql(allNewRegisSql).show();

        /**
         * +-----------+
         * |allNewRegis|
         * +-----------+
         * |        100|
         * +-----------+
         */

        // 新用户的注册比例
        long allNewView = (long) spark.sql(allNewViewSql).collectAsList().get(0).get(0);
        long allNewRegis = (long) spark.sql(allNewRegisSql).collectAsList().get(0).get(0);

        double rate = (double) allNewRegis / (double) allNewView;

        System.out.println(Tools.formatDouble(rate,2));

        return rate;

        /**
         * 0.1
         */
    }

    /**
     * 用户跳出率：IP只浏览了一个页面就离开网站的次数/网站总访问数（PV）
     * 1、先获取各网站总访问数（PV）
     * 2、再获取每个网站只有userid浏览一次的次数
     * 3、求出用户跳出率，统计再一张表上
     * @param date
     * @param spark
     */
    public static Dataset<Row> calculateUserLeaveRate(String date, SparkSession spark){

        String sql = "select t1.cdate,t1.pageid,t1.pv_count,t2.unique_count,round((t2.unique_count / t1.pv_count),2) rate from " +
                "(select cdate,pageid,count(pageid) pv_count from hivespark.news_access " +
                "where action = 'view' and cdate = '"+date+"' group by cdate,pageid) t1 " +
                "left join " +
                "(select cdate,pageid,count(count) unique_count from " +
                "(select cdate,pageid,count(userid) count from hivespark.news_access " +
                "where action = 'view' and cdate = '"+date+"' group by cdate,pageid,userid)" +
                "t where count = 1 group by cdate,pageid) t2 " +
                "on t1.pageid = t2.pageid order by t2.unique_count desc limit 10";

        spark.sql(sql).show();

        return spark.sql(sql);

        /**
         * +----------+------+--------+------------+----+
         * |     cdate|pageid|pv_count|unique_count|rate|
         * +----------+------+--------+------------+----+
         * |2020-12-12|   462|      23|          23| 1.0|
         * |2020-12-12|   732|      18|          18| 1.0|
         * |2020-12-12|   467|      18|          18| 1.0|
         * |2020-12-12|   437|      18|          18| 1.0|
         * |2020-12-12|   585|      18|          18| 1.0|
         * |2020-12-12|   623|      20|          18| 0.9|
         * |2020-12-12|   719|      21|          18|0.86|
         * |2020-12-12|   285|      17|          17| 1.0|
         * |2020-12-12|     9|      19|          17|0.89|
         * |2020-12-12|    96|      17|          17| 1.0|
         * +----------+------+--------+------------+----+
         */

    }

    /**
     * 版块热度排行榜：根据每个版块每天被访问的次数，做出一个排行榜
     * @param date
     * @param spark
     */
    public static Dataset<Row> calculateSectionSort(String date, SparkSession spark){

        // 写法一
        String sql = "select cdate,section,se_count from " +
                "(select cdate,section,count(section) se_count from hivespark.news_access " +
                "where action = 'view' and cdate = '"+date+"' group by cdate,section) " +
                "t order by se_count desc";

        // 写法二
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("select cdate,section,se_count from ");
        stringBuilder.append(" ( ");
        stringBuilder.append(" select cdate,section,count(section) se_count from hivespark.news_access ");
        stringBuilder.append(" where action = 'view' and cdate = '"+date+"' group by cdate,section ");
        stringBuilder.append(" ) ");
        stringBuilder.append(" t order by se_count desc ");

        spark.sql(stringBuilder.toString()).show();

        return spark.sql(stringBuilder.toString());

        /**
         * +----------+-------------+--------+
         * |     cdate|      section|se_count|
         * +----------+-------------+--------+
         * |2020-12-12|   technology|     733|
         * |2020-12-12|      fashion|     711|
         * |2020-12-12|        funny|     687|
         * |2020-12-12|international|     684|
         * |2020-12-12|     internet|     678|
         * |2020-12-12|       carton|     678|
         * |2020-12-12|     aviation|     676|
         * |2020-12-12|        movie|     669|
         * |2020-12-12|   government|     657|
         * |2020-12-12|        sport|     646|
         * |2020-12-12|     military|     644|
         * |2020-12-12|          car|     644|
         * |2020-12-12|      tv-show|     642|
         * |2020-12-12|      country|     630|
         * |2020-12-12|entertainment|     621|
         * +----------+-------------+--------+
         */

    }

    /**
     * 将分析出的结果数据写入到mysql数据库中
     * @param dataset
     * @param tablename
     */
    public static void writeData(Dataset<Row> dataset,String tablename){

        dataset.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://bigdata-pro-m04/spark")
                .option("dbtable", tablename)
                .option("user", "root")
                .option("password", "199911")
                .save();

    }
}
