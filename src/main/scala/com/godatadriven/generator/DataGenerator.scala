package com.godatadriven.generator

import com.godatadriven.common.Config
import com.godatadriven.generator.UniformDataGenerator.KeyLabel
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

trait DataGenerator {


  def numberOfRows(numberOfKeys: Int = Config.numberOfKeys,
                   keysMultiplier: Int = Config.keysMultiplier): Long =
    generateSkewedSequence(numberOfKeys).map(_._2).sum * keysMultiplier.toLong

  /**
    * Generates a sequence of numbers, for example num = 22 would generate:
    * Array(22, 10, 6, 4, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
    *
    * @param numberOfKeys number of elements in the sequence
    * @return tuple of (key, num), where key is the unique key and num is the times it will be repeated
    */
  def generateSkewedSequence(numberOfKeys: Int): List[(Int, Int)] =
    (0 to numberOfKeys).par.map(i =>
      (i, Math.ceil(
        (numberOfKeys.toDouble - i.toDouble) / (i.toDouble + 1.0)
      ).toInt)
    ).toList

  def createMediumTable(spark: SparkSession, tableName: String, numberOfPartitions: Int): Unit = {

    import spark.implicits._

    val df = spark
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
            Math.floor(r.nextDouble() * Config.numberOfBroadcastPasses).toInt
          )
        )
      })
      .repartition(numberOfPartitions)

    assert(df.count() == Config.numberOfKeys)

    df
      .write
      .mode(SaveMode.Overwrite)
      .parquet(tableName)
  }

  def buildTestset(spark: SparkSession,
                   numberOfKeys: Int = Config.numberOfKeys,
                   keysMultiplier: Int = Config.keysMultiplier,
                   numberOfPartitions: Int = Config.numberOfPartitions): Unit

  def getName: String

  def getMediumTableName: String

  def getLargeTableName: String

}
