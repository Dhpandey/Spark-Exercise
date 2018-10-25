package sparksql

import org.apache.spark.sql.{SQLContext, SparkSession}
import utils.AccessLogs

/**
  * Uses SparkSql
  * 1. get data and parse using utility class to create RDD
  * 2. Calculates content size, min max and average
  * 3.Counts Response code
  * 4 Counts IPAddress and show that is accessed more then 10 times
  * 5.Counts Endpoints and order them on the basis of count using custom ordering class
  * Note :CustomOrdering and AccessLogParser are two utilities used.
  *
  * How to Run:
  * spark-submit --class apache.accesslogsSQL.ServerLogAnalyzerSQL --master
  * local ScalaSpark/Scala1/target/scala-2.10/Scala1-assembly-1.0.jar > output.txt
  */

class ServerLogAnalyzerSQL {

  def contentSize(sqlContext: SQLContext) = {
    val contentSize = sqlContext.sql("SELECT SUM(contentSize), COUNT(*),MIN(contentSize)," +
      " MAX(contentSize)" + " from AccessLogTable").first()

    println("Content SIze :: Average : %s , Min:  %s Max : %s"
      .format(contentSize.getLong(0) / contentSize.getLong(1), contentSize(2), contentSize(3)))
  }

  def responseCodeCount(sqLContext: SQLContext) {
    val responseCount = sqLContext.sql("SELECT responseCode,COUNT(*) from AccessLogTable" +
      " GROUP BY responseCode LIMIT 1000").collect()

    println( s"""ResponseCode :: ${responseCount.mkString("[", ",", "]")}""")
  }

  def inAddressFilter(sqlContext: SQLContext) = {
    import sqlContext.implicits._

    val result = sqlContext.sql("SELECT ipAddr, COUNT(*) AS total from AccessLogTable " +
      "GROUP BY ipAddr HAVING total>1")
      //.map(row => row.getString(0))  // just to collect only IP
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()
    println( s"""IP address :: ${result.mkString("[", ",", "]")}""")

  }

  def countAndOrderEndPoints(sqlContext: SQLContext) = {

    import sqlContext.implicits._
    val result = sqlContext.sql("SELECT endPoint, COUNT(*) AS total from AccessLogTable " +
      "GROUP BY endPoint ORDER BY total DESC LIMIT 10 ")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()
    println( s"""End Point Calculation :: ${result.mkString("[", ",", "]")}""")
  }

}

object ServerLogAnalyzerSQL {
  def main(args: Array[String]) {


    val logObj = new ServerLogAnalyzerSQL
    val spark = SparkSession
      .builder()
      .appName("SparkExercise")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.sqlContext.implicits._


    val logs = spark.sparkContext.textFile("src/main/resources/access_log/access_log.txt").map(log => AccessLogs.logParser(log)).toDF()

    logs.createOrReplaceTempView("AccessLogTable")
    spark.sqlContext.cacheTable("AccessLogTable")

    logObj.contentSize(spark.sqlContext)
    logObj.responseCodeCount(spark.sqlContext)
    logObj.inAddressFilter(spark.sqlContext)
    logObj.countAndOrderEndPoints(spark.sqlContext)

  }
}
