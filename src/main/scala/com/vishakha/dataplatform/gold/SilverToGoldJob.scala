package com.vishakha.dataplatform.gold

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SilverToGoldJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Silver To Gold")
      .master("local[*]")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val silverDF = Seq(
      (101, "login"),
      (101, "login"),
      (102, "logout"),
      (103, "purchase")
    ).toDF("user_id", "event_type")

    val goldDF = silverDF
      .groupBy("event_type")
      .agg(
        count("*").as("total_events"),
        countDistinct("user_id").as("unique_users")
      )
      .withColumn("metric_date", current_date())

    goldDF.show(false)

    spark.stop()
  }
}
