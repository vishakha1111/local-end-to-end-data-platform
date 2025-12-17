package com.vishakha.dataplatform

import org.apache.spark.sql.SparkSession

object TestSpark {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      (1, "Vishakha"),
      (2, "AWS Data Engineer")
    ).toDF("id", "name")

    df.show()

    spark.stop()
  }
}
