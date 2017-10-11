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
        |SELECT large.key, COUNT(medium.label)
        |FROM large
        |JOIN medium ON medium.key = large.key
        |GROUP BY large.key
        |ORDER BY COUNT(*) DESC
      """.stripMargin).show(22)
  }
}
