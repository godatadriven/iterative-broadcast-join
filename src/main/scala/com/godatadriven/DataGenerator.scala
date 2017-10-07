package com.godatadriven

import breeze.stats.distributions.Exponential
import com.godatadriven.common.Config
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}

object DataGenerator {

  def buildTestset(): Unit = {
    val spark = Utils.getSpark


    def schema: StructType = {
      StructType(StructField("key", LongType, nullable = false) :: Nil)
    }

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    def sampleExponentialDistribution(numbers: Iterator[Int]): Iterator[Long] = {
      val exp = new Exponential(Config.randomExponent)
      val samples = exp.sample(numbers.length)

      samples
        .map(sample => Math.round(sample * Config.numberOfKeys))
        .toIterator
    }

    val rdd = spark
      .sparkContext
      .parallelize(1 to Config.numberOfRows, Config.numberOfPartitions)
      .mapPartitions(sampleExponentialDistribution)
      .map(key => Row(key))

    val dfLarge = spark.createDataFrame(rdd, schema)

    dfLarge
      .write
      .mode(SaveMode.Overwrite)
      .save("table_large.parquet")

    // Get an idea of the distribution
    //df.groupBy("key").count().orderBy(desc("count")).coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("skewed-data")

    val dfMedium = dfLarge.distinct().map(row => (row.getAs[Long](0), s"label-${row.get(0)}")).toDF("key", "label")

    dfMedium
      .write
      .mode(SaveMode.Overwrite)
      .save("table_medium.parquet")
  }

}
