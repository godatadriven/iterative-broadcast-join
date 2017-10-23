package com.godatadriven

import com.godatadriven.common.Config
import com.godatadriven.generator.SkewedDataGenerator
import org.apache.spark.sql.SparkSession


object RunBenchmark extends App {
  def getSparkSession(appName: String = "Spark Application"): SparkSession = {
    val spark = SparkSession
      .builder
      .appName(appName)
      .getOrCreate()

    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("parquet.enable.dictionary", "false")
    spark.conf.set("spark.default.parallelism", Config.numberOfPartitions)
    spark.conf.set("spark.sql.shuffle.partitions", Config.numberOfPartitions)

    // Tell Spark to don't be too chatty
    spark.sparkContext.setLogLevel("WARN")

    spark
  }

  (0 to 10).foreach(step => {

    // Increment the multiplier stepwise
    val multiplier = Config.keysMultiplier + (step * Config.keysMultiplier)

    val rows = SkewedDataGenerator.numberOfRows(Config.numberOfKeys, multiplier)

    var spark = getSparkSession(s"Generate dataset with ${Config.numberOfKeys} keys, $rows rows")

    // cache java.lang.OutOfMemoryError
    SkewedDataGenerator.buildTestset(
      spark,
      keysMultiplier = multiplier
    )

    spark.stop()
    spark = null
    spark = getSparkSession(s"Iterative broadcast join $multiplier")

    RunTest.run(spark, "itr")

    spark.stop()
    spark = null
    spark = getSparkSession(s"Sort-merge-join $multiplier")

    try {
      RunTest.run(spark, "std")
    } catch {
      case e: Exception => println(s"Got ${e}")
    }

    spark.stop()
    spark = null
  })
}
