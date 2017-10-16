package com.godatadriven

import com.godatadriven.join.IterativeBroadcastJoin
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object RunTest {
  def run() {
    val spark = Utils.getSpark

    val result = IterativeBroadcastJoin.join(
      spark,
      spark
        .read
        .load("table_large.parquet"),
      spark
        .read
        .load("table_medium.parquet")
    )

    result
      .write
      .mode(SaveMode.Overwrite)
      .save("table_result.parquet")

    val dfRes = spark
      .read
      .parquet("table_result.parquet")

    val nullValues = dfRes
      .filter("id is null")
      .count()

    // Make sure that the join went well
    assert(nullValues == 0)

    dfRes
      .groupBy("key")
      .count()
      .orderBy(desc("count"))
      .show(22)
  }
}
