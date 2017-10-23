package com.godatadriven

import com.godatadriven.common.Config
import com.godatadriven.join.{IterativeBroadcastJoin, NormalJoin}
import org.apache.spark.sql.{SaveMode, SparkSession}

object RunTest {
  def run(spark: SparkSession,
          joinType: String = Config.joinType,
          tableNameMedium: String,
          tableNameLarge: String,
          tableNameOutput: String) {

    val result = joinType match {
      case "std" => NormalJoin.join(
        spark,
        spark
          .read
          .load(tableNameLarge),
        spark
          .read
          .load(tableNameMedium)
      )
      case "itr" => IterativeBroadcastJoin.join(
        spark,
        spark
          .read
          .load(tableNameLarge),
        spark
          .read
          .load(tableNameMedium)
      )
      case _ => throw new RuntimeException("Could not derive join strategy")
    }

    result
      .write
      .mode(SaveMode.Overwrite)
      .parquet(tableNameOutput)
  }
}
