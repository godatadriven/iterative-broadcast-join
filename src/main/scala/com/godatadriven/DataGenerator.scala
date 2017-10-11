package com.godatadriven

import com.godatadriven.common.Config
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}

object DataGenerator {

  // To .. to .., or .. util .., scala iterator does not support numbers >= Int.MaxValue
  def bigIterator(end: Long): Iterator[Long] =
    Iterator.iterate(1L)(_ + 1L).takeWhile(_ <= end)

  def buildTestset(): Unit = {
    val spark = Utils.getSpark

    def schema: StructType = StructType(StructField("key", LongType, nullable = false) :: Nil)

    import spark.implicits._

    def sampleExponentialDistribution(numSamples: Int): Iterable[Long] =
      new NormalDistribution(0.0, 0.1925)
        .sample(numSamples)
        .map(sample => Math.round(Math.abs(sample * Config.numberOfKeys)))

    val numberOfSamplesPerPartition = List.fill(Config.numberOfPartitions) {
      (Config.numberOfRows / Config.numberOfPartitions).toInt
    }

    val rdd = spark
      .sparkContext
      .parallelize(numberOfSamplesPerPartition, Config.numberOfPartitions)
      .flatMap(sampleExponentialDistribution)
      .map(key => Row(key))

    val dfLarge = spark.createDataFrame(rdd, schema)

    dfLarge
      .write
      .mode(SaveMode.Overwrite)
      .save("table_large.parquet")

//    println(s"Number of rows: ${dfLarge.count()}")
//    println(s"Number of keys: ${dfLarge.distinct().count()}")

    // Get an idea of the distribution, by dumping it into csv and load it into plotly
    dfLarge
      .groupBy("key")
      .count()
      .orderBy(desc("count"))
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("skewed-data")

    spark
      .read
      .parquet("table_large.parquet")
      .distinct()
      .map(row => (row.getAs[Long](0), s"label-${row.get(0)}", s"description telling something about id ${row.get(0)}"))
      .toDF("key", "label", "description")
      .limit(1000)
      .write
      .mode(SaveMode.Overwrite)
      .save("table_medium.parquet")
  }

}
