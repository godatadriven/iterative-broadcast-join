package com.godatadriven

import com.godatadriven.common.Config
import com.godatadriven.dataframe.join.NormalJoin
import com.godatadriven.join.{IterativeBroadcastJoin, NormalJoin}
import org.apache.spark.sql.{SaveMode, SparkSession}

object RunTest {
  def run(spark: SparkSession,
          joinType: String = Config.joinType) {

    val result = joinType match {
      case "std" => NormalJoin.join(
        spark,
        spark
          .read
          .load(Config.tableNameLarge),
        spark
          .read
          .load(Config.tableNameMedium)
      )
      case "itr" => IterativeBroadcastJoin.join(
        spark,
        spark
          .read
          .load(Config.tableNameLarge),
        spark
          .read
          .load(Config.tableNameMedium)
      )
      case _ => throw new RuntimeException("Could not derive join strategy")
    }

    result
      .write
      .mode(SaveMode.Overwrite)
      .parquet("result.parquet")
  }
}
