package com.vishakha.dataplatform

import com.vishakha.dataplatform.utils.SparkSessionUtil

object TestSpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionUtil.getSparkSession("TestSpark")

    import spark.implicits._

    val df = Seq(
      (1, "Alice"),
      (2, "Bob")
    ).toDF("id", "name")

    df.show()

    spark.stop()
  }
}
