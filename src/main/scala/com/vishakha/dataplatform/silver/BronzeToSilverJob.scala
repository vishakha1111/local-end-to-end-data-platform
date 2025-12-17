package com.vishakha.dataplatform.silver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BronzeToSilverJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Bronze To Silver")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val bronzeDF = Seq(
      (1, "login", 101),
      (2, "logout", 102),
      (3, "login", 101),
      (3, "login", 101)
    ).toDF("event_id", "event_type", "user_id")

    val silverDF = bronzeDF
      .dropDuplicates("event_id")
      .filter(col("event_type").isNotNull)

    silverDF.show(false)

    spark.stop()
  }
}
