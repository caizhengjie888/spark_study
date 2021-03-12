package com.kfk.spark.traffic_analysis_project;

import java.io.Serializable;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/30
 * @time : 6:40 下午
 */
public class LogInfo implements Serializable {
    private long timeStamp;
    private long upTraffic;
    private long downTraffic;

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStame(long timeStame) {
        this.timeStamp = timeStame;
    }

    public long getUpTraffic() {
        return upTraffic;
    }

    public void setUpTraffic(long upTraffic) {
        this.upTraffic = upTraffic;
    }

    public long getDownTraffic() {
        return downTraffic;
    }

    public void setDownTraffic(long downTraffic) {
        this.downTraffic = downTraffic;
    }

    public LogInfo(){

    }

    public LogInfo(long timeStame, long upTraffic, long downTraffic) {
        this.timeStamp = timeStame;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }
}
