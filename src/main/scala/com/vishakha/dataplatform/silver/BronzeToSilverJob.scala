package com.vishakha.dataplatform.silver

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object BronzeToSilverJob {

  def cleanBronzeData(bronzeDF: DataFrame): DataFrame = {
    bronzeDF
      .filter(col("event_id").isNotNull)
      .filter(col("event_type").isNotNull)
      .filter(col("user_id").isNotNull)
      .dropDuplicates("event_id")
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Bronze To Silver")
      .master("local[*]")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val bronzeDF = Seq(
      (1, "login", 101, "2025-01-01"),
      (2, "logout", 102, "2025-01-01"),
      (3, "login", 101, "2025-01-01"),
      (3, "login", 101, "2025-01-01"), // duplicate
      (4, "purchase", 103, "2025-01-01")
    ).toDF("event_id", "event_type", "user_id", "processing_date")

    val silverDF = cleanBronzeData(bronzeDF)
      .withColumn("is_valid_event",
        col("event_type").isin("login", "logout", "purchase")
      )
      .filter(col("is_valid_event") === true)
      .drop("is_valid_event")

    silverDF.show(false)

    spark.stop()
  }
}
