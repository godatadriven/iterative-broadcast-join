package com.godatadriven.join

import org.apache.spark.sql.{DataFrame, SparkSession}

object NormalJoin extends JoinStrategy {

  override def join(spark: SparkSession,
                    join: JoinType,
                    dfLarge: DataFrame,
                    dfMedium: DataFrame): DataFrame = {

    // Explicitly disable the broadcastjoin
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    dfLarge
      .join(
        dfMedium,
        Seq("key"),
        "left_outer"
      )
      .select(
        dfLarge("key"),
        dfMedium("label")
      )
  }
}
