package com.godatadriven

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RunTest {
  def run() {
    val spark = Utils.getSpark

    def registerTable(name: String): Unit =
      spark
        .read
        .load(s"table_$name.parquet")
        .createOrReplaceTempView(name)

    registerTable("large")
    registerTable("medium")

    spark.sql(
      """
        |SELECT
        | large.key           AS key,
        | medium.label        AS label,
        | medium.description  AS description
        |FROM large
        |JOIN medium ON medium.key = large.key
      """.stripMargin)
      .write
      .mode(SaveMode.Overwrite)
      .save("result.parquet")


    spark
      .read
      .parquet("table_large.parquet")
      .groupBy("key")
      .count()
      .orderBy(desc("count"))
      .show(22)
  }
}
