package com.kfk.spark.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/18
 * @time : 7:26 下午
 */
public class ConnectionPool {

    private static LinkedList<Connection> connectionQueue;

    /**
     * 加载驱动
     */
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接，多线程访问并发控制
     * @return
     */
    public synchronized static Connection getConnection(){
        try {
            if (connectionQueue == null){
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0;i < 10;i++){
                    Connection conn = DriverManager.getConnection(
                            "jdbc:mysql://bigdata-pro-m04:3306/spark?useSSL=false",
                            "root",
                            "199911"
                    );
                    connectionQueue.push(conn);
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    /**
     * 还回去一个连接
     * @param conn
     */
    public static void returnConnection(Connection conn){
        connectionQueue.push(conn);
    }
}
