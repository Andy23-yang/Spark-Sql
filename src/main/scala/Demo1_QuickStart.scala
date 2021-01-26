import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
object Demo1_QuickStart {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    //1. SparkSession
    val spark = SparkSession.builder()
      .appName("Demo1_QuickStart")
      .master("local[*]")
      .getOrCreate()
    //2. 加载数据
    val pdf:DataFrame = spark.read.json("file:///E:\\IntelliJProject\\Spark-Sql\\data\\people.json")

    //3.打印表结构
    //pdf.printSchema()

    //4.查询数据
    pdf.show()

    //5. select name, age from people
    //pdf.select("name", "age").show()

    //6. 列计算
    import spark.implicits._
    //pdf.select($"name", $"age" + 10).show

    //7.列计算，取别名
    //pdf.select($"name", ($"age" + 10).as("age")).show

    //8.聚合函数，统计不同年龄的人数,先分组再聚合
    //pdf.select($"age").groupBy($"age").count().as("count").show()

    //9.条件查询，获取年龄超过18岁的人的信息
    pdf.select("name", "age", "height").where($"age" > 18).show()
  }
}
