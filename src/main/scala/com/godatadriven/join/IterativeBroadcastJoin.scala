package com.godatadriven.join

import com.godatadriven.common.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object IterativeBroadcastJoin extends JoinStrategy {
  override def join(spark: SparkSession,
                    dfLarge: DataFrame,
                    dfMedium: DataFrame): DataFrame = {

    dfMedium
      // Add a new column and make sure that it is \in [0, Config.numberOfPasses]
      .withColumn("pass", floor(rand() * Config.broadcastIterations))
      .write
      .partitionBy("pass")
      .mode(SaveMode.Overwrite)
      .save("medium_broadcast.parquet")


    var result = dfLarge
      .withColumn("label", lit(null))
      .withColumn("description", lit(null))

    for (pass <- 0 until Config.broadcastIterations) {
      val dfMediumSliced =
        spark
          .read
          .parquet("medium_broadcast.parquet")

      result = result.join(
        broadcast(dfMediumSliced),
        result("key") === dfMediumSliced("key") && dfMediumSliced("pass") === lit(pass),
        "leftouter"
      ).select(
        result("key"),

        // Join in the label
        coalesce(
          result("label"),
          dfMediumSliced("label")
        ).as("label"),

        // Join in the description
        coalesce(
          result("description"),
          dfMediumSliced("description")
        ).as("description")
      )
    }

    result
  }
}
