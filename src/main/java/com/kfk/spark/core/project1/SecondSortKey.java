package com.kfk.spark.core.project1;

import scala.Serializable;
import scala.math.Ordered;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/1
 * @time : 11:23 上午
 */
public class SecondSortKey implements Ordered<SecondSortKey> , Serializable {

    private String first;
    private int second;

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public SecondSortKey(){

    }

    public SecondSortKey(String first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compare(SecondSortKey that) {
        int comp = this.getFirst().compareTo(that.getFirst());
        if (comp == 0){
            return Integer.valueOf(this.getSecond()).compareTo(that.getSecond());
        }

        return comp;
    }

    @Override
    public boolean $less(SecondSortKey that) {
        return false;
    }

    @Override
    public boolean $greater(SecondSortKey that) {
        return false;
    }

    @Override
    public boolean $less$eq(SecondSortKey that) {
        return false;
    }

    @Override
    public boolean $greater$eq(SecondSortKey that) {
        return false;
    }

    @Override
    public int compareTo(SecondSortKey that) {
        int comp = this.getFirst().compareTo(that.getFirst());
        if (comp == 0){
            return Integer.valueOf(this.getSecond()).compareTo(that.getSecond());
        }

        return comp;
    }
}
