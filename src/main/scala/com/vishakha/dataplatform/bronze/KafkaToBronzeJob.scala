package com.vishakha.dataplatform.bronze

import com.vishakha.dataplatform.utils.SparkSessionUtil
import org.apache.spark.sql.functions._

object KafkaToBronzeJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionUtil.getSparkSession("KafkaToBronze")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "events-topic")
      .option("startingOffsets", "earliest")
      .load()

    val parsedDF = kafkaDF
      .selectExpr("CAST(value AS STRING) as message")
      .withColumn("ingestion_ts", current_timestamp())

    parsedDF.writeStream
      .format("console")
      .option("truncate", "false")
      .option("path", "file:///C:/spark-data/bronze/events")
      .start()
      .awaitTermination()
  }
}
