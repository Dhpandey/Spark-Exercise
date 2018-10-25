package sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.{AccessLogs, CustomOrdering}

class ServerLogAnalyzer {
  def calcContentSize(log: RDD[AccessLogs]) = {
    val size = log.map(log => log.contentSize).cache()
    val average = size.reduce(_ + _) / size.count()
    println("ContentSize:: Average :: " + average + " " +
      " || Maximum :: " + size.max() + "  || Minimum ::" + size.min())
  }

  def responseCodeCount(log: RDD[AccessLogs]) = {
    val responseCount = log.map(log => (log.responseCode, 1))
      .reduceByKey(_ + _)
      .take(1000)
    println( s"""ResponseCodes Count : ${responseCount.mkString("[", ",", "]")} """)
  }

  def ipAddressFilter(log: RDD[AccessLogs]) = {
    val result = log.map(log => (log.ipAddr, 1))
      .reduceByKey(_ + _)
      .filter(count => count._2 > 1)
      // .map(_._1).take(10)
      .collect()

    println( s"""Ip Addresses :: ${result.mkString("[", ",", "]")}""")
  }

  def manageEndPoints(log: RDD[AccessLogs]) = {
    val result = log.map(log => (log.endPoint, 1))
      .reduceByKey(_ + _)
      .top(10)(CustomOrdering.SecondValueSorting)

    println( s"""EndPoints :: ${result.mkString("[", ",", "]")}""")
  }
}

object ServerLogAnalyzer {
  def main(args: Array[String]) {
    val logObj = new ServerLogAnalyzer
    val spark = SparkSession
      .builder()
      .appName("SparkExercise")
      .config("spark.master", "local")
      .getOrCreate()

    val semiStructuredLogs = spark.sparkContext.textFile("src/main/resources/access_log/access_log.txt")

    val logs = semiStructuredLogs.map(logFile => AccessLogs.logParser(logFile)).filter(x => x != null).cache()

    logObj.calcContentSize(logs)
    logObj.responseCodeCount(logs)
    logObj.ipAddressFilter(logs)
    logObj.manageEndPoints(logs)

  }

}
