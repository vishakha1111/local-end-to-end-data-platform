package com.vishakha.dataplatform.utils

import org.apache.spark.sql.SparkSession

object SparkSessionUtil {

  def getSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }

}
