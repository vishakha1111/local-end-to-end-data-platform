//source to bronze
package com.vishakha.dataplatform.bronze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BatchEventsToBronze {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Batch Events To Bronze")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val eventsDF = Seq(
      (1, "login", 101),
      (2, "logout", 102),
      (3, "login", 101),
      (4, "purchase", 103)
    ).toDF("event_id", "event_type", "user_id")

    eventsDF.show(false)

    spark.stop()
  }
}
