package com.godatadriven.join

import org.apache.spark.sql.{DataFrame, SparkSession}

trait JoinStrategy {

  def join(spark: SparkSession,
           join: JoinType,
           dfLarge: DataFrame,
           dfMedium: DataFrame): DataFrame

}
