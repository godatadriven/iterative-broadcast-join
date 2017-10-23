package com.godatadriven

import com.godatadriven.common.Config
import com.godatadriven.generator.{DataGenerator, SkewedDataGenerator, UniformDataGenerator}
import com.godatadriven.join._
import org.apache.spark.sql.{SaveMode, SparkSession}


object RunBenchmark extends App {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000 / 1000 / 1000 + " sec")
    result
  }


  def runTest(generator: DataGenerator,
              joinType: JoinType,
              tableNameOutput: String) {

    val rows = generator.numberOfRows()

    val name = s"${generator.getName}: $joinType, passes=${Config.numberOfBroadcastPasses}, keys=${Config.numberOfKeys}, multiplier=${Config.keysMultiplier}, rows=$rows"

    println(name)


    val spark = getSparkSession(name)

    time {

      val out = joinType match {
        case _: SortMergeJoinType => NormalJoin.join(
          spark,
          spark
            .read
            .load(generator.getLargeTableName),
          spark
            .read
            .load(generator.getMediumTableName)
        )
        case _: IterativeBroadcastJoinType => IterativeBroadcastJoin.join(
          spark,
          spark
            .read
            .load(generator.getLargeTableName),
          spark
            .read
            .load(generator.getMediumTableName)
        )
      }

      out.write
        .mode(SaveMode.Overwrite)
        .parquet(tableNameOutput)
    }

    spark.stop()
  }


  def runBenchmark(dataGenerator: DataGenerator,
                   iterations: Int = 8,
                   outputTable: String = "result.parquet"): Unit = {
    val originalMultiplier = Config.keysMultiplier

    (0 to iterations)
      .map(step => originalMultiplier + (step * originalMultiplier))
      .foreach(multiplier => {

        val keys = Config.numberOfKeys
        Config.keysMultiplier = multiplier

        // Generate uniform data and benchmark
        val rows = dataGenerator.numberOfRows()

        val spark = getSparkSession(s"${dataGenerator.getName}: Generate dataset with $keys keys, $rows rows")
        dataGenerator.buildTestset(
          spark,
          keysMultiplier = multiplier
        )
        spark.stop()

        Config.numberOfBroadcastPasses = 2

        runTest(
          dataGenerator,
          new IterativeBroadcastJoinType,
          outputTable
        )

        Config.numberOfBroadcastPasses = 3

        runTest(
          dataGenerator,
          new IterativeBroadcastJoinType,
          outputTable
        )

        runTest(
          dataGenerator,
          new SortMergeJoinType,
          outputTable
        )
      })

    // Reset global Config
    Config.keysMultiplier = originalMultiplier
  }

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

//  runBenchmark(UniformDataGenerator)
  runBenchmark(SkewedDataGenerator)
}
