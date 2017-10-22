package com.godatadriven

import co.theasi.plotly._
import com.godatadriven.common.Config
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


object RunBenchmark extends App {

  def time(block: => Unit): Double = {
    val t0 = System.nanoTime()
    block
    val t1 = System.nanoTime()
    (t1 - t0).toDouble
  }

  def getSparkSession(appName: String = "Spark Application"): SparkSession = {
    val spark = SparkSession
      .builder
      .appName(appName)
      .getOrCreate()

    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("parquet.enable.dictionary", "false")
    spark.conf.set("spark.broadcast.blockSize", "1m")
    spark.conf.set("spark.default.parallelism", Config.numberOfPartitions)
    spark.conf.set("spark.sql.shuffle.partitions", Config.numberOfPartitions)
    // Tell Spark to don't be too chatty
    spark.sparkContext.setLogLevel("WARN")

    spark
  }

  val resultsItr = mutable.Map[Long, Double]()
  val resultsStd = mutable.Map[Long, Double]()

  (0 to 10).foreach(step => {

    // Steps of 10%
    val multiplier = Config.keysMultiplier + (step * Config.keysMultiplier)

    val rows = DataGenerator.numberOfRows(Config.numberOfKeys, multiplier)

    var spark = getSparkSession(s"Generate dataset with ${Config.numberOfKeys} keys, $rows rows")

    // cache java.lang.OutOfMemoryError
    DataGenerator.buildTestset(
      spark,
      keysMultiplier = multiplier
    )

    spark.stop()
    spark = null
    spark = getSparkSession(s"Iterative broadcast join $multiplier")

    resultsItr(rows) = time {
      RunTest.run(spark, "itr")
    }

    Thread.sleep(2000000)

    spark.stop()
    spark = null
    spark = getSparkSession(s"Sort-merge-join $multiplier")

    try {

      resultsStd(rows) = time {
        RunTest.run(spark, "std")
      }
    } catch {
      case e: Exception => println(s"Got ${e}")
    }

    spark.stop()
    spark = null
  })

  // Get an idea of the distribution, by dumping it into csv and load it into plotly

  val commonOptions = ScatterOptions()

  val x1 = resultsItr.keys.map(_.toDouble)
  val y1 = resultsItr.values

  val x2 = resultsStd.keys.map(_.toDouble)
  val y2 = resultsStd.values
  val plot = Plot()
    .withScatter(x1, y1, commonOptions.name("Iterative broadcast join"))
    .withScatter(x2, y2, commonOptions.name("Sort merge join"))

  draw(plot, "benchmark-multiplier")

}
