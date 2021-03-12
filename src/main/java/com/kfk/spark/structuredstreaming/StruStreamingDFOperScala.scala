package com.kfk.spark.structuredstreaming

import com.kfk.spark.common.CommSparkSessionScala

import java.sql.Date
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType;

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/22
 * @time : 7:31 下午
 */
object StruStreamingDFOperScala {
    case  class  DeviceData(device : String,deviceType : String , signal : Double ,deviceTime: Date)
    def main(args: Array[String]): Unit = {
        val spark = CommSparkSessionScala.getSparkSession() ;

        import spark.implicits._;

        // input table
        val socketDF = spark
                .readStream
                .format("socket")
                .option("host", "bigdata-pro-m04")
                .option("port", 9999)
                .load().as[String]

        // 构造Schema
        val userSchema = new StructType()
                .add("device", "string")
                .add("deviceType", "string")
                .add("signal", "double")
                .add("deviceTime", "date")

        // 根据schema转换为dataset
        val df_device = socketDF.map(x => {
            val lines = x.split(",")
            Row(lines(0),lines(1),lines(2),lines(3))
        })(RowEncoder(userSchema))

        // 根据case  class转换为dataset
        val df : Dataset[DeviceData] = df_device.as[DeviceData]

        // result table
        val df_final = df.groupBy("deviceType").count()

        // output
        val query = df_final.writeStream
                .outputMode("update")
                .format("console")
                .start()

        query.awaitTermination()
    }
}
