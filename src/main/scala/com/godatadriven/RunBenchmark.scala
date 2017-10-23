package com.godatadriven

import com.godatadriven.benchmark.Benchmark
import com.godatadriven.common.Config
import com.godatadriven.generator.{SkewedDataGenerator, UniformDataGenerator}
import org.apache.spark.sql.SparkSession


object RunBenchmark extends App {
  def getSparkSession(appName: String = "Spark Application"): SparkSession = {
    val spark = SparkSession
      .builder
      .master("local[*]")
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

  Benchmark.runBenchmark(iterations = 8, UniformDataGenerator)

  Benchmark.runBenchmark(iterations = 8, SkewedDataGenerator)
}
