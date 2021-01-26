import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object demo {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession
      .builder()
      .appName("demo")
      .master("local[*]")
      .getOrCreate()

    val courseDF : DataFrame = spark.read.json("file:///E:\\IntelliJProject\\Spark-Sql\\data\\course.json")
    val scoreDF : DataFrame = spark.read.json("file:///E:\\IntelliJProject\\Spark-Sql\\data\\score.json")
    val student : DataFrame = spark.read.json("file:///E:\\IntelliJProject\\Spark-Sql\\data\\student.json")
    val teacher : DataFrame = spark.read.json("file:///E:\\IntelliJProject\\Spark-Sql\\data\\teacher.json")

    //-- 1、查询"01"课程比"02"课程成绩高的学生的信息及课程分数
    val dfTest_1 : DataFrame = scoreDF.join(scoreDF, Seq("s_id"), "left")
    val dfTest_2 : Dataset[Row] = dfTest_1.filter(
      x=>x.get(1).equals("01") &&
         x.get(3).equals("02") &&
         java.lang.Double.parseDouble(
           x.get(2).toString
         ))
    dfTest_1.show()
  }
}
