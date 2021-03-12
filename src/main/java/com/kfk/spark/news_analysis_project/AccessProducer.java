package com.kfk.spark.news_analysis_project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * 访问日志Kafka Producer
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/12
 * @time : 7:51 下午
 */
public class AccessProducer extends Thread{

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static String date;

    // 版块内容
    private static String[] sections = new String[] {"country", "international", "sport",
            "entertainment", "movie", "carton",
            "tv-show", "technology", "internet",
            "car", "military", "funny",
            "fashion", "aviation", "government"};

    private static Random random = new Random();

    private static int[] newOldUserArr = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    private Producer<String,String> producer;
    private String topic;

    /**
     * 构造函数
     * @param topic
     */
    public AccessProducer(String topic){
        this.topic = topic;
        producer = new KafkaProducer<String, String>(createProducerConfig());
        date = simpleDateFormat.format(new Date());
    }

    /**
     * createProducerConfig
     * @return
     */
    public Properties createProducerConfig(){
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", "bigdata-pro-m04:9092");
        return properties;
    }

    @Override
    public void run(){
        int counter = 0;

        while (true){
            // 生成1000条访问数据
            for (int i = 0;i < 1000;i++){
                String log = null;

                // 生成条访问数据
                if (newOldUserArr[random.nextInt(10)] == 1){
                    log = getAccessLog();
                } else {
                    log = getRegisterLog();
                }

                // 将数据发送给kafka
                producer.send(new ProducerRecord<String, String>(topic,log));

                counter++;
                if (counter == 100){
                    counter = 0;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * 生成注册数据
     * @return
     */
    private static String getRegisterLog(){

        StringBuffer stringBuffer = new StringBuffer("");

        // 生成时间戳
        long timestamp = System.currentTimeMillis();

        // 随机生成userid（默认1000注册用户，每天1/10的访客是未注册用户）
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

        return stringBuffer.append(date).append(" ")
                .append(timestamp).append(" ")
                .append(userid).append(" ")
                .append(pageid).append(" ")
                .append(section).append(" ")
                .append(action).toString();
    }

    /**
     * 生成访问数据
     * @return
     */
    private static String getAccessLog(){

        StringBuffer stringBuffer = new StringBuffer("");

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

        return stringBuffer.append(date).append(" ")
                .append(timestamp).append(" ")
                .append(userid).append(" ")
                .append(pageid).append(" ")
                .append(section).append(" ")
                .append(action).toString();
    }

    public static void main(String[] args) {
        AccessProducer accessProducer = new AccessProducer("spark");
        accessProducer.start();
    }
}
