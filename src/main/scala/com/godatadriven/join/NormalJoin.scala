package com.godatadriven.join

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object NormalJoin extends JoinStrategy {
  override def join(spark: SparkSession, dfLarge: DataFrame, dfMedium: DataFrame) = {
    dfLarge
  }
}
