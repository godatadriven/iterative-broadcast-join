package com.godatadriven.join

import com.godatadriven.SparkUtil
import com.godatadriven.common.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec

object IterativeBroadcastJoin extends JoinStrategy {

  @tailrec
  private def iterativeBroadcastJoin(spark: SparkSession,
                                     result: DataFrame,
                                     broadcast: DataFrame,
                                     iteration: Int = 0): DataFrame =
    if (iteration < Config.numberOfBroadcastPasses) {
      val tableName = s"tmp_broadcast_table_itr_$iteration.parquet"

      val out = result.join(
        broadcast.filter(col("pass") === lit(iteration)),
        Seq("key"),
        "left_outer"
      ).select(
        result("key"),

        // Join in the label
        coalesce(
          result("label"),
          broadcast("label")
        ).as("label")
      )

      SparkUtil.dfWrite(out, tableName)

      iterativeBroadcastJoin(
        spark,
        SparkUtil.dfRead(spark, tableName),
        broadcast,
        iteration + 1
      )
    } else result

  override def join(spark: SparkSession,
                    dfLarge: DataFrame,
                    dfMedium: DataFrame): DataFrame = {
    broadcast(dfMedium)
    iterativeBroadcastJoin(
      spark,
      dfLarge
        .select("key")
        .withColumn("label", lit(null)),
      dfMedium
    )
  }

}
