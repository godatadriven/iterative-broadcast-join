package com.godatadriven

import com.godatadriven.common.Config
import com.godatadriven.join.{IterativeBroadcastJoin, NormalJoin}
import org.apache.spark.sql.SparkSession

object RunTest {
  def run(spark: SparkSession) {

    val result = Config.joinType match {
      case "std" => NormalJoin.join(
        spark,
        spark
          .read
          .load("table_large.parquet"),
        spark
          .read
          .load("table_medium.parquet")
      )
      case "itr" => IterativeBroadcastJoin.join(
        spark,
        spark
          .read
          .load("table_large.parquet"),
        spark
          .read
          .load("table_medium.parquet")
      )
      case _ => throw new RuntimeException("Could not derive join strategy")
    }

    val nullValues = result
      .filter("label is null")
      .count()

    // Make sure that the join went well
    assert(nullValues == 0)
  }
}
