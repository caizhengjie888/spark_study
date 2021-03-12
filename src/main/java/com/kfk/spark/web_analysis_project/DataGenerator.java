package com.kfk.spark.web_analysis_project;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * 离线数据生成器
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/12
 * @time : 7:51 下午
 */
public class DataGenerator {
    public static void main(String[] args) {

        StringBuffer stringBuffer = new StringBuffer();

        // 版块内容
        String[] sections = new String[] {"country", "international", "sport",
                "entertainment", "movie", "carton",
                "tv-show", "technology", "internet",
                "car", "military", "funny",
                "fashion", "aviation", "government"};

        Random random = new Random();
        int[] newOldUserArr = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        // 生成日期，默认是昨天
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_YEAR,-1);
        Date yesterday = calendar.getTime();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String date = simpleDateFormat.format(yesterday);

        // 生成10000条访问数据
        for (int i = 0; i < 10000; i++){
            // 生成时间戳
            long timestamp = System.currentTimeMillis();

            // 随机生成userid（默认10000注册用户，每天1/10的访客是未注册用户）
            Long userid = 0L;
            int newOldUser = newOldUserArr[random.nextInt(10)];
            if (newOldUser == 1){
                userid = null;
            } else {
                userid = (long)random.nextInt(100000);
            }

            // 随机生成pageid，共1000个页面
            long pageid = random.nextInt(1000);

            // 随机生成板块
            String section = sections[random.nextInt(sections.length)];

            // 生成固定的行为，view
            String action = "view";

            stringBuffer.append(date).append(",")
                    .append(timestamp).append(",")
                    .append(userid).append(",")
                    .append(pageid).append(",")
                    .append(section).append(",")
                    .append(action).append("\n");
        }

        // 生成100条注册数据
        for (int i = 0; i < 100 ; i++ ){
            // 生成时间戳
            long timestamp = System.currentTimeMillis();

            // 新用户都是userid为null
            Long userid = null;

            // 生成随机pageid，都是null
            Long pageid = null;

            // 生成随机版块，都是null
            String section = null;

            // 生成固定的行为，view
            String action = "register";

            stringBuffer.append(date).append(",")
                    .append(timestamp).append(",")
                    .append(userid).append(",")
                    .append(pageid).append(",")
                    .append(section).append(",")
                    .append(action).append("\n");
        }

        PrintWriter printWriter = null;

        try {
            printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream("/Users/caizhengjie/Desktop/new_access_"+date+".log")));
            printWriter.write(stringBuffer.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (printWriter != null){
                printWriter.close();
            }
        }
    }
}
