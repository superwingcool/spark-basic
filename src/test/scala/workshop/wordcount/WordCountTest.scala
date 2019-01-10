package workshop.wordcount

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

class WordCountTest extends FlatSpec with Matchers with LocalSparkSession {

  "word count" should "right" in {
    WordCount.run(spark, "data/wordcount.txt", "data/wordcountcsv");
    val wc = spark.read.csv("data/wordcountcsv").collect()
    wc.size should equal(7)
    wc should equal(Array(
      Row("a", "3"),
      Row("b", "2"),
      Row("c", "3"),
      Row("d", "3"),
      Row("e", "1"),
      Row("f", "2"),
      Row("g", "1")
    ))

  }

}
