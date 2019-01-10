package workshop.wordcount

import java.time.Clock

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WordCount {
  val log: Logger = LogManager.getRootLogger
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    log.setLevel(Level.INFO)

    val spark = SparkSession.builder.appName("Spark Word Count").getOrCreate()
    //    val spark = SparkSession.builder()
    //      .appName("Spark Word Count")
    //      .master("local[2]")
    //      .getOrCreate();
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val filePath = args(0);

    val outputPath = args(1);

    run(spark, filePath, outputPath)

    spark.stop()
  }

  def run(spark: SparkSession, filePath: String, outputPath: String): Unit = {
    log.info("Reading data: ")
    log.info("Writing data: ")
    spark.read
      .text(filePath)
      .withColumn("word", explode(split(col("value"), " ")))
      .select("word")
      .groupBy("word")
      .count()
      .orderBy("word")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("quoteAll", false)
      .option("quote", " ")
      .csv(outputPath)
    //df.show()
    log.info("Application Done: " + spark.sparkContext.appName)
  }
}
