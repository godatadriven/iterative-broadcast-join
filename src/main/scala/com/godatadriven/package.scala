package com.godatadriven

import org.apache.spark.sql.SparkSession

package object Utils {
  def getSpark: SparkSession = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Example iterative broadcast join")
      .getOrCreate()

    // Tell Spark to don't be too chatty
    spark.sparkContext.setLogLevel("WARN")

    spark
  }
}
