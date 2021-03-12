package com.kfk.spark.web_analysis_project;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/12
 * @time : 9:42 下午
 */
public class Tools {

    /**
     * 获取yesterday时间：yyyy-MM-dd
     * @return
     */
    public static String getYesterday(){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_YEAR,-1);
        Date yesterday = calendar.getTime();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        return simpleDateFormat.format(yesterday);
    }

    /**
     * 数据的小数点处理
     * @param num
     * @param scale
     * @return
     */
    public static double formatDouble(double num, int scale){
        BigDecimal bigDecimal = new BigDecimal(num);
        return bigDecimal.setScale(scale,BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
