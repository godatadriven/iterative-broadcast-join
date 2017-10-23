package com.godatadriven

import com.godatadriven.common.Config
import com.godatadriven.generator.{DataGenerator, SkewedDataGenerator, UniformDataGenerator}
import com.godatadriven.join._
import org.apache.spark.sql.{SaveMode, SparkSession}


object RunBenchmark extends App {


  def runTest(spark: SparkSession,
              joinType: JoinType,
              tableNameMedium: String,
              tableNameLarge: String,
              tableNameOutput: String) {
    val out = joinType match {
      case join: SortMergeJoinType => NormalJoin.join(
        spark,
        join,
        spark
          .read
          .load(tableNameLarge),
        spark
          .read
          .load(tableNameMedium)
      )
      case join: IterativeBroadcastJoinType => IterativeBroadcastJoin.join(
        spark,
        join,
        spark
          .read
          .load(tableNameLarge),
        spark
          .read
          .load(tableNameMedium)
      )
    }

    out.write
      .mode(SaveMode.Overwrite)
      .parquet(tableNameOutput)
  }


  def runBenchmark(dataGenerator: DataGenerator, iterations: Int = 8): Unit =
    (0 to iterations).foreach(step => {

      val keys = Config.numberOfKeys

      // Increment the multiplier stepwise
      val multiplier = Config.keysMultiplier + (step * Config.keysMultiplier)

      // Generate uniform data and benchmark
      val rows = dataGenerator.numberOfRows(Config.numberOfKeys, multiplier)

      var spark = getSparkSession(s"${dataGenerator.getName}: Generate dataset with $keys keys, $rows rows")

      dataGenerator.buildTestset(
        spark,
        keysMultiplier = multiplier
      )

      spark.stop()
      spark = null
      spark = getSparkSession(s"${dataGenerator.getName}: Iterative broadcast join keys=$keys, multiplier=$multiplier, rows=$rows")

      runTest(
        spark,
        new IterativeBroadcastJoinType(2),
        dataGenerator.getMediumTableName,
        dataGenerator.getLargeTableName,
        "result.parquet"
      )

      spark.stop()
      spark = null
      spark = getSparkSession(s"${dataGenerator.getName}: Sort-merge-join keys=$keys, multiplier=$multiplier, rows=$rows")

      runTest(
        spark,
        new SortMergeJoinType,
        dataGenerator.getMediumTableName,
        dataGenerator.getLargeTableName,
        "result.parquet"
      )

      spark.stop()
      spark = null
    })

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

  runBenchmark(UniformDataGenerator)
  runBenchmark(SkewedDataGenerator)
}
