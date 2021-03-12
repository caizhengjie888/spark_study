package com.kfk.spark.structuredstreaming;

import java.sql.Timestamp;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/22
 * @time : 9:25 下午
 */
public class EventData {

    //timestamp: Timestamp,
    //word: String

    private Timestamp wordtime;
    private String word;

    public Timestamp getWordtime() {
        return wordtime;
    }

    public void setWordtime(Timestamp wordtime) {
        this.wordtime = wordtime;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public EventData() {
    }

    public EventData(Timestamp wordtime, String word) {
        this.wordtime = wordtime;
        this.word = word;
    }
}
