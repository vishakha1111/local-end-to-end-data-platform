package com.vishakha.dataplatform.utils

import org.apache.spark.sql.SparkSession

object SparkSessionUtil {
  def create(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }
}
