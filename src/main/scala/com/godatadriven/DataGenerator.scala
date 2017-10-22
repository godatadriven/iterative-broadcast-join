package com.godatadriven

import com.godatadriven.common.Config
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

object DataGenerator {

  case class Key(key: Int)

  case class KeyLabel(key: Int, label: String, pass: Int)

  /**
    * Generates a sequence of numbers, for example num = 22 would generate:
    * Array(22, 10, 6, 4, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
    *
    * @param numberOfKeys number of elements in the sequence
    * @return as sequence of numbers
    */
  def generateSkewedSequence(numberOfKeys: Int): List[(Int, Int)] =
    (0 to numberOfKeys).par.map(i =>
      (i, Math.round(
        (numberOfKeys.toDouble - i.toDouble) / (i.toDouble + 1.0)
      ).toInt)
    ).toList

  def numberOfRows(numberOfKeys: Int, keysMultiplier: Int): Long =
    (0 to numberOfKeys).map(i =>
      Math.round(
        (numberOfKeys.toDouble - i.toDouble) / (i.toDouble + 1.0)
      )
    ).sum * keysMultiplier


  /**
    * Will generate a sequence of the input sample
    *
    * @param key   The sample and the
    * @param count count the number of repetitions
    * @return
    */
  def skewDistribution(key: Int, count: Int): Seq[Int] = {
    Seq.fill(count)(key)
  }

  def buildTestset(spark: SparkSession,
                   numberOfKeys: Int = Config.numberOfKeys,
                   keysMultiplier: Int = Config.keysMultiplier,
                   numberOfPartitions: Int = Config.numberOfPartitions): Unit = {

    import spark.implicits._

    println(s"Generating ${numberOfRows(numberOfKeys, keysMultiplier)} rows")

    spark
      .sparkContext
      .parallelize(generateSkewedSequence(numberOfKeys), numberOfPartitions)
      .flatMap(list => (0 to keysMultiplier).map(_ => list))
      .repartition(numberOfPartitions)
      .flatMap(pair => skewDistribution(pair._1, pair._2))
      .toDS()
      .map(Key)
      .repartition(numberOfPartitions)
      .write
      .mode(SaveMode.Overwrite)
      .save("table_large.parquet")

    // Get an idea of the distribution, by dumping it into csv and load it into plotly
    //    val dist =
    //      spark
    //        .read
    //        .parquet("table_large.parquet")
    //        .groupBy("key")
    //        .count()
    //        .map(row => row.getAs[Int](0) -> row.getAs[Long](1))
    //        .rdd
    //        .takeOrdered(1000)(Ordering[Int].on { x => x._1 })
    //
    //    val x = dist.map(_._1)
    //    val y = dist.map(_._2.toDouble)
    //    val plot = Plot().withScatter(x, y)
    //
    //    draw(plot, "iterative-broadcast-dist")

    spark
      .read
      .parquet("table_large.parquet")
      .as[Int]
      .distinct()
      .mapPartitions(rows => {
        val r = new Random()
        rows.map(key =>
          KeyLabel(
            key,
            s"Description for entry $key, that can be anything",
            // Already preallocate the pass of the broadcast iteration here
            Math.floor(r.nextDouble() * Config.broadcastIterations).toInt
          )
        )
      })
      .repartition(numberOfPartitions)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("table_medium.parquet")
  }

}
