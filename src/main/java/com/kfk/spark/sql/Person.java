package com.kfk.spark.sql;

import java.io.Serializable;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/2
 * @time : 10:15 下午
 */
public class Person implements Serializable {
    private String name;
    private long age;

    public Person(String name, long age) {
        this.name = name;
        this.age = age;
    }

    public Person() {

    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }
}
