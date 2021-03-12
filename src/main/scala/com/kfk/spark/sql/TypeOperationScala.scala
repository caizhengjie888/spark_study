package com.kfk.spark.sql

import com.kfk.spark.common.{Comm, CommSparkSessionScala}
import org.apache.spark.sql.SparkSession

/**
 * @author : 蔡政洁
 * @email :caizhengjie888@icloud.com
 * @date : 2020/12/4
 * @time : 2:45 下午
 */
object TypeOperationScala {

    // case class Person(name:String, age:Option[Long])
    case class Person(name:String, age:Long)
    case class Employee(name1:String, salary:Long)
    def main(args: Array[String]): Unit = {
        val spark = CommSparkSessionScala.getSparkSession()

        /**
         * coalesce / repartition
         */
        coalesceAndRepartitions(spark)

        /**
         * distinct / dropDuplicates
         */
        distinctAndDropDuplicates(spark)

        /**
         * filter / except / intersect
         */
        filterAndExceptAndIntersect(spark)

        /**
         * map / flatMap / mapPartition
         */
        mapAndFlatMap(spark)

        /**
         * join
         */
        joinWith(spark)

        /**
         * sort
         */
        sort(spark)

        /**
         * sample / randomSplit
         */
        sampleAndRandomSplit(spark)
    }

    /**
     * coalesce 和 repartition 操作
     * 都是用来进行重分区的
     * 区别在于：coalesce只能用于减少分区的数据，而且可以选择不发生shuffle
     * repartition可以增加分区的数据，也可以减少分区的数据，必须会发生shuffle，相当于进行了一次重新分区操作
     * @param spark
     */
    def coalesceAndRepartitions(spark : SparkSession): Unit ={

        val personDF = spark.read.json(Comm.fileDirPath + "people.json")
        // 1
        println(personDF.rdd.partitions.size)

        // repartition
        val personDFRepartition = personDF.repartition(4)
        // 4
        println(personDFRepartition.rdd.partitions.size)

        // coalesce
        val personDFCoalesce = personDFRepartition.coalesce(3)
        // 3
        println(personDFCoalesce.rdd.partitions.size)
    }

    /**
     * distinct：是根据每一条数据进行完整内容的对比和去重
     * DropDuplicates：可以根据指定的字段进行去重
     * @param spark
     */
    def distinctAndDropDuplicates(spark : SparkSession): Unit ={

        val personDF = spark.read.json(Comm.fileDirPath + "people.json")
        personDF.show()

        /**
         * +----+-------+
         * | age|   name|
         * +----+-------+
         * |null|Michael|
         * |  30|   Andy|
         * |  19| Justin|
         * |  19| Justin|
         * |  32| Justin|
         * +----+-------+
         */

        // distinct
        personDF.distinct().show()

        /**
         * +----+-------+
         * | age|   name|
         * +----+-------+
         * |null|Michael|
         * |  30|   Andy|
         * |  19| Justin|
         * |  32| Justin|
         * +----+-------+
         */

        // dropDuplicates
        personDF.dropDuplicates(Seq("name")).show()

        /**
         * +----+-------+
         * | age|   name|
         * +----+-------+
         * |null|Michael|
         * |  30|   Andy|
         * |  19| Justin|
         * +----+-------+
         */

    }

    /**
     * filter：根据我们自己的业务逻辑，如果返回true，就保留该元素，如果是false，就不保留该元素
     * except：获取在当前的dataset中有，但是在另一个dataset中没有的元素
     * intersect：获取两个dataset中的交集
     * @param spark
     */
    def filterAndExceptAndIntersect(spark : SparkSession): Unit ={

        import spark.implicits._
        val personDS = spark.read.json(Comm.fileDirPath + "people.json").as[Person]
        val personDS1 = spark.read.json(Comm.fileDirPath + "people1.json").as[Person]

        // filter方式一
        personDS.filter(line => line.age != None).show()

        /**
         * +---+------+
         * |age|  name|
         * +---+------+
         * | 30|  Andy|
         * | 19|Justin|
         * | 19|Justin|
         * | 32|Justin|
         * +---+------+
         */

        // filter方式二
        val personDF = spark.read.json(Comm.fileDirPath + "people.json")
        val rdd = personDF.rdd.filter(line => line.getAs("age") != null)
        for (elem <- rdd.collect()) {
            System.out.println(elem)
        }

        /**
         * [30,Andy]
         * [19,Justin]
         * [19,Justin]
         * [32,Justin]
         */

        // except
        personDS.except(personDS1).show()

        /**
         * +----+-------+
         * | age|   name|
         * +----+-------+
         * |null|Michael|
         * |  30|   Andy|
         * |  19| Justin|
         * +----+-------+
         */

        // intersect
        personDS.intersect(personDS1).show()

        /**
         * +---+------+
         * |age|  name|
         * +---+------+
         * | 32|Justin|
         * +---+------+
         */
    }

    /**
     * map：将数据集中的每一条数据做一个映射，返回一条映射
     * flatMap：数据集中的每条数据都可以返回多条数据
     * mapPartition：一次性对一个partition中的数据进行处理
     * @param spark
     */
    def mapAndFlatMap(spark : SparkSession): Unit ={

        import spark.implicits._
        val personDS = spark.read.json(Comm.fileDirPath + "people.json").as[Person]

        // map
        personDS.filter("age is not null").map(line => (line.name,line.age+10)).show()

        /**
         * +------+---+
         * |    _1| _2|
         * +------+---+
         * |  Andy| 40|
         * |Justin| 29|
         * |Justin| 29|
         * |Justin| 42|
         * +------+---+
         */

        // flatMap
        personDS.filter("age is not null").flatMap(line => {
            Seq(Person(line.name + "_1",line.age + 1000),Person(line.name + "_2",line.age + 1000))
        }).show()

        /**
         * +--------+----+
         * |    name| age|
         * +--------+----+
         * |  Andy_1|1030|
         * |  Andy_2|1030|
         * |Justin_1|1019|
         * |Justin_2|1019|
         * |Justin_1|1019|
         * |Justin_2|1019|
         * |Justin_1|1032|
         * |Justin_2|1032|
         * +--------+----+
         */

        // mapPartition
        personDS.filter("age is not null").mapPartitions(line => {
            var result = scala.collection.mutable.ArrayBuffer[(String, Long)]()
            while (line.hasNext){
                var person = line.next()
                result += ((person.name,person.age + 1000))
            }
            result.iterator
        }).show()

        /**
         * +------+----+
         * |    _1|  _2|
         * +------+----+
         * |  Andy|1030|
         * |Justin|1019|
         * |Justin|1019|
         * |Justin|1032|
         * +------+----+
         */

    }

    /**
     * join：根据某一个字段，关联合并两个数据集
     * @param spark
     */
    def joinWith(spark : SparkSession): Unit ={

        import spark.implicits._
        val peopleFilePath = Comm.fileDirPath + "people.json"
        val employeeFilePath = Comm.fileDirPath + "employees.json"

        val personDS = spark.read.json(peopleFilePath).as[Person]
        val employeeDS = spark.read.json(employeeFilePath).as[Employee]

        personDS.joinWith(employeeDS,$"name" === $"name1").show()

        /**
         * +------------+---------------+
         * |          _1|             _2|
         * +------------+---------------+
         * | [, Michael]|[Michael, 3000]|
         * |  [30, Andy]|   [Andy, 4500]|
         * |[32, Justin]| [Justin, 3500]|
         * |[19, Justin]| [Justin, 3500]|
         * |[19, Justin]| [Justin, 3500]|
         * +------------+---------------+
         */
    }

    /**
     * sort：对数据表排序
     * @param spark
     */
    def sort(spark : SparkSession): Unit ={
        import spark.implicits._
        val employeeFilePath = Comm.fileDirPath + "employees.json"
        val employeeDS = spark.read.json(employeeFilePath).as[Employee]
        employeeDS.show()

        /**
         * +-------+------+
         * |  name1|salary|
         * +-------+------+
         * |Michael|  3000|
         * |   Andy|  4500|
         * | Justin|  3500|
         * |  Berta|  4000|
         * +-------+------+
         */

        employeeDS.sort($"salary".desc).show()

        /**
         * +-------+------+
         * |  name1|salary|
         * +-------+------+
         * |   Andy|  4500|
         * |  Berta|  4000|
         * | Justin|  3500|
         * |Michael|  3000|
         * +-------+------+
         */

        // 创建临时表，使用sql语句
        employeeDS.toDF().createOrReplaceTempView("employee")
        val result = spark.sql("select * from employee a where 1=1 order by salary desc")
        result.show()

        /**
         * +-------+------+
         * |  name1|salary|
         * +-------+------+
         * |   Andy|  4500|
         * |  Berta|  4000|
         * | Justin|  3500|
         * |Michael|  3000|
         * +-------+------+
         */

    }

    /**
     * randomSplit：根据Array中的元素的数量进行切分，然后再给定每个元素的值来保证对切分数据的权重
     * sample：根据我们设定的比例进行随机抽取
     * @param spark
     */
    def sampleAndRandomSplit(spark : SparkSession): Unit ={
        import spark.implicits._
        val employeeFilePath = Comm.fileDirPath + "employees.json"
        val employeeDS = spark.read.json(employeeFilePath).as[Employee]

        employeeDS.randomSplit(Array(2,5,1)).foreach(ds => ds.show())

        /**
         * +-----+------+
         * |name1|salary|
         * +-----+------+
         * |Berta|  4000|
         * +-----+------+
         *
         * +------+------+
         * | name1|salary|
         * +------+------+
         * |  Andy|  4500|
         * |Justin|  3500|
         * +------+------+
         *
         * +-------+------+
         * |  name1|salary|
         * +-------+------+
         * |Michael|  3000|
         * +-------+------+
         */

        employeeDS.sample(false,0.5).show()

        /**
         * +-------+------+
         * |  name1|salary|
         * +-------+------+
         * |Michael|  3000|
         * | Justin|  3500|
         * +-------+------+
         */
    }
}
