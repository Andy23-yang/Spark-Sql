import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}

object demo1 {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("demo1")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val peopleDF = spark.read.json("file:///E:\\IntelliJProject\\Spark-Sql\\data\\people.json")
    peopleDF.show()
    peopleDF.printSchema() //打印结构信息
    peopleDF.describe("name").show() //获取指定字段的统计信息
    peopleDF.collect()//获取所有数据到数组
    peopleDF.collectAsList() //获取所有数据到List

    //查询操作
    peopleDF.where("name='Andy' or age=19").show()
    peopleDF.select("name").show() //展示所有人的名字

    //执行sql语句
    //1.首先需要将DataFrame注册为临时表才可以在该表上执行sql语句
    peopleDF.createTempView("people")
    val sqlDF = spark.sql(
      """
        |select name, height
        |from people
        |where height >= 170 and height <= 190
        |""".stripMargin)
    sqlDF.show()

    //2.注册为全局临时表
    peopleDF.createGlobalTempView("global_people")
    spark.newSession().sql(
      """
        |select name, height
        |from global_temp.global_people
        |where height >= 170 and height <= 190
        |""".stripMargin).show()

    //3.重复建表会报错: Temporary view 'global_people' already exists
    //peopleDF.createGlobalTempView("global_people")

    //4. 保存DataFrame为其他格式
    // parquet是spark sql读取的默认格式
    peopleDF.write.parquet("E:\\IntelliJProject\\Spark-Sql\\data\\people.parquet")

    //5. 读取
    val tempDF = spark.read.load("E:\\IntelliJProject\\Spark-Sql\\data\\people.parquet")
    tempDF.show()
  }
}