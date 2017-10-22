package com.godatadriven

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkUtil {

  def dfWrite(df: DataFrame, name: String): Unit =
    df
      .write
      .mode(SaveMode.Overwrite)
      .parquet(name)

  def dfRead(spark: SparkSession, name: String): DataFrame =
    spark
      .read
      .load(name)

}
