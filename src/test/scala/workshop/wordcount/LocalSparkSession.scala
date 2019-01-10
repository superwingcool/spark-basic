package workshop.wordcount

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait LocalSparkSession extends BeforeAndAfterAll {

  self: Suite =>

  @transient var spark: SparkSession = _

  override def beforeAll() {
    spark = SparkSession.builder()
      .appName("Spark Test")
      .master("local[2]")
      .getOrCreate();
  }

  override def afterAll() {
    if (spark != null) {
      spark.stop()
    }
  }

}
