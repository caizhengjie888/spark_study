package com.kfk.spark.core

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/11/28
 * @time : 1:27 下午
 */
class SecondSortKeyScala(val first : String,val second : Int) extends Ordered[SecondSortKeyScala] with Serializable {

    override def compare(that: SecondSortKeyScala): Int = {
        val comp = this.first.compareTo(that.first)
        if (comp == 0){
            return this.second.compareTo(that.second)
        }
        return comp;
    }
}
