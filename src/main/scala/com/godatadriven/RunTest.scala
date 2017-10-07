package com.godatadriven

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
        |SELECT medium.key, COUNT(*)
        |FROM large
        |JOIN medium ON medium.key = large.key
        |GROUP BY medium.key
        |ORDER BY COUNT(*) DESC
      """.stripMargin).show(22)
  }
}
