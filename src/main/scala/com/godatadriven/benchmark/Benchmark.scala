package com.godatadriven.benchmark

import com.godatadriven.RunBenchmark.getSparkSession
import com.godatadriven.RunTest
import com.godatadriven.common.Config
import com.godatadriven.generator.DataGenerator

object Benchmark {

  def runBenchmark(iterations: Int, dataGenerator: DataGenerator): Unit = {

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

      RunTest.run(spark,
        "itr",
        dataGenerator.getMediumTableName,
        dataGenerator.getLargeTableName,
      "result.parquet")

      spark.stop()
      spark = null
      spark = getSparkSession(s"${dataGenerator.getName}: Sort-merge-join keys=$keys, multiplier=$multiplier, rows=$rows")

      try {
        RunTest.run(spark,
          "std",
          dataGenerator.getMediumTableName,
          dataGenerator.getLargeTableName,
          "result.parquet")
      } catch {
        case e: Exception => println(s"Got $e")
      }

      spark.stop()
      spark = null
    })
  }
}
