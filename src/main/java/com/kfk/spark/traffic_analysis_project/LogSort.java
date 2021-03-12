package com.kfk.spark.traffic_analysis_project;

import scala.Serializable;
import scala.math.Ordered;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/30
 * @time : 7:39 下午
 */
public class LogSort extends LogInfo implements Ordered<LogSort> , Serializable {
    private long timeStamp;
    private long upTraffic;
    private long downTraffic;

    @Override
    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public long getUpTraffic() {
        return upTraffic;
    }

    @Override
    public void setUpTraffic(long upTraffic) {
        this.upTraffic = upTraffic;
    }

    @Override
    public long getDownTraffic() {
        return downTraffic;
    }

    @Override
    public void setDownTraffic(long downTraffic) {
        this.downTraffic = downTraffic;
    }

    public LogSort(){

    }
    public LogSort(long timeStamp, long upTraffic, long downTraffic) {
        this.timeStamp = timeStamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    @Override
    public int compare(LogSort that) {
        int comp = Long.valueOf(this.getUpTraffic()).compareTo(that.getUpTraffic());
        if (comp == 0){
            comp = Long.valueOf(this.getDownTraffic()).compareTo(that.getDownTraffic());
        }
        if (comp == 0){
            comp = Long.valueOf(this.getTimeStamp()).compareTo(that.getTimeStamp());
        }
        return comp;
    }

    @Override
    public boolean $less(LogSort that) {
        return false;
    }

    @Override
    public boolean $greater(LogSort that) {
        return false;
    }

    @Override
    public boolean $less$eq(LogSort that) {
        return false;
    }

    @Override
    public boolean $greater$eq(LogSort that) {
        return false;
    }

    @Override
    public int compareTo(LogSort that) {
        int comp = Long.valueOf(this.getUpTraffic()).compareTo(that.getUpTraffic());
        if (comp == 0){
            comp = Long.valueOf(this.getDownTraffic()).compareTo(that.getDownTraffic());
        }
        if (comp == 0){
            comp = Long.valueOf(this.getTimeStamp()).compareTo(that.getTimeStamp());
        }
        return comp;
    }
}
