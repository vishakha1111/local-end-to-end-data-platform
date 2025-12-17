package com.vishakha.dataplatform.gold

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SilverToGoldJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Silver To Gold")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val silverDF = Seq(
      (101, "login"),
      (101, "login"),
      (102, "logout"),
      (103, "purchase")
    ).toDF("user_id", "event_type")

    val goldDF = silverDF
      .groupBy("event_type")
      .count()

    goldDF.show(false)

    spark.stop()
  }
}
