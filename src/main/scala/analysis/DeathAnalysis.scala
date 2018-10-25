package analysis

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

//spark-submit --class analysis.DeathAnalysis --master local /Users/Dheeraj/IdeaProjects/Spark-Exercise/target/scala-2.11/spark-exercise_2.11-1.0.jar /Users/Dheeraj/IdeaProjects/Spark-Exercise/src/main/resources/death_cause.csv /Users/Dheeraj/IdeaProjects/Spark-Exercise/src/main/resources/yearAndDeath  /Users/Dheeraj/IdeaProjects/Spark-Exercise/src/main/resources/yearAndState
//spark-submit --class analysis.DeathAnalysis --master local /Users/Dheeraj/IdeaProjects/Spark-Exercise/target/scala-2.11/spark-exercise_2.11-1.0.jar
//spark-submit --class analysis.DeathAnalysis --master local /Users/Dheeraj/Documents/spark-exercise_2.11-1.0.jar
object DeathAnalysis {
  def main(args: Array[String]) {
    if (args.length < 2) {
      throw new Exception("Enter source and output location")
    }
    val death_cause = args(0)
    val yearAndDeath = args(1)
    val yearAndState = args(2)

//    val death_cause = "hdfs://localhost:9001/user/Dheeraj/deathData.csv"
//    val yearAndDeath = "hdfs://localhost:9001/user/Dheeraj/yearAndDeath"
//    val yearAndState = "hdfs://localhost:9001/user/Dheeraj/yearAndState"

    val spark = SparkSession
      .builder()
      .appName("SparkExercise")
      .config("spark.master", "local")
      .getOrCreate()

    val data = spark.read.format("csv").schema(customSchema).load(death_cause)

    getCauseByYearAndState(data, yearAndState)
    getReasonByYear(data, yearAndDeath)
  }

  private def getReasonByYear(data: DataFrame, outputLocation: String) = {
    import org.apache.spark.sql.functions._
    val result = data.select("year", "rate", "cause", "state", "deaths")
      .where("cause !=  'All causes'")
      .groupBy("year", "cause")
      .sum("deaths")
      .orderBy(desc("sum(deaths)"))

    result.coalesce(1)
      .write.option("header", true)
      .mode("overwrite")
      .csv(outputLocation)
  }

  private def getCauseByYearAndState(data: DataFrame, outputLocation: String) = {
    import org.apache.spark.sql.functions._
    val result = data.select("year", "rate", "cause", "state", "deaths")
      .where("cause !=  'All causes'")
      .groupBy("year", "state", "cause")
      .sum("deaths")
      .orderBy(desc("sum(deaths)"))

    result.coalesce(1)
      .write
      .option("header", true)
      .mode("overwrite")
      .csv(outputLocation)
  }

  val customSchema = StructType(Array(
    StructField("year", StringType, true),
    StructField("113_cause", StringType, true),
    StructField("cause", StringType, true),
    StructField("state", StringType, true),
    StructField("deaths", IntegerType, true),
    StructField("rate", StringType, true)
  ))
}
