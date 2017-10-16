package com.godatadriven.join

import org.apache.spark.sql.{DataFrame, SparkSession}

object NormalJoin extends JoinStrategy {
  override def join(spark: SparkSession, dfLarge: DataFrame, dfMedium: DataFrame) =
    dfLarge
      .join(dfMedium, dfLarge("key") === dfMedium("key"))
      .select(
        dfLarge("key"),
        dfMedium("label"),
        dfMedium("description")
      )
}
