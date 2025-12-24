package com.vishakha.dataplatform.bronze

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BatchEventsToBronze {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Batch Events To Bronze")
      .master("local[*]")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val eventSchema = StructType(List(
      StructField("event_id", IntegerType, nullable = false),
      StructField("event_type", StringType, nullable = false),
      StructField("user_id", IntegerType, nullable = false)
    ))

    val rawData = Seq(
      Row(1, "login", 101),
      Row(2, "logout", 102),
      Row(3, "login", 101),
      Row(4, "purchase", 103)
    )

    val bronzeDF = (spark createDataFrame(
      spark.sparkContext.parallelize(rawData),
      eventSchema
    ))
      .withColumn("ingestion_ts", current_timestamp())
      .withColumn("source_system", lit("mock_events"))
      .withColumn("processing_date", current_date())

    bronzeDF.show(false)

    spark.stop()
  }
}
