package com.vishakha.dataplatform.utils

import org.apache.spark.sql.SparkSession

object SparkSessionUtil {

  def getSparkSession(appName: String): SparkSession = {

    System.setProperty("hadoop.home.dir", "C:/tmp")
    System.setProperty("java.library.path", "")

    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      // 🔥 Disable Hadoop native IO (CRITICAL for Windows)
      .config("spark.hadoop.io.native.lib.available", "false")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }
}
