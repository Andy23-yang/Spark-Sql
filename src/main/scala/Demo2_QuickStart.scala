import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo2_QuickStart {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    //1. SparkSession
    val spark = SparkSession.builder()
      .appName("Demo2_QuickStart")
      .master("local[*]")
      .getOrCreate()
    //2. 加载数据
    val pdf:DataFrame = spark.read.json("file:///E:\\IntelliJProject\\Spark-Sql\\data\\people.json")

    //3.sql
    /*
    * registerTempTable: 使用createOrReplaceTempView替代
    * 创建表/视图
    * createTempView
    * createOrReplaceTempView
    * createGlobalTempView
    * createOrReplaceGlobalTempView
    *
    * 从使用范围上来说，分为带global和不带的函数：
    * 1. 带global是当前spark application中可用，不带表示只能在当前的spark session中可用
    *
    * 从创建的角度说，分为带replace和不带的
    * 1. 带replace的，表示创建视图， 如果视图存在会覆盖之前的数据。反之，视图不存在就创建，如果视图存在就报错
    */
    pdf.createOrReplaceTempView("people")
    spark.sql(
      """
        |select
        |age,
        |count(1)
        |height
        |from people
        |group by age
        |""".stripMargin).show()
    spark.stop()
  }
}
