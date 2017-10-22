package com.godatadriven.dataframe.join

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

trait JoinStrategy {

  def join(spark: SparkSession,
           dfLarge: DataFrame,
           dfMedium: DataFrame): DataFrame

}
