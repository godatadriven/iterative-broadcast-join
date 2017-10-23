package com.godatadriven.generator

import com.godatadriven.common.Config
import org.apache.spark.sql.{SaveMode, SparkSession}

object UniformDataGenerator extends DataGenerator {


  def buildTestset(spark: SparkSession,
                   numberOfKeys: Int = Config.numberOfKeys,
                   keysMultiplier: Int = Config.keysMultiplier,
                   numberOfPartitions: Int = Config.numberOfPartitions): Unit = {

    import spark.implicits._

    val numRows = numberOfRows(numberOfKeys, keysMultiplier)

    println(s"Generating $numRows rows")

    spark
      .range(keysMultiplier)
      .repartition(numberOfPartitions)
      .flatMap(_ => 0 to numberOfKeys)
      .map(Key)
      .repartition(numberOfPartitions)
      .write
      .mode(SaveMode.Overwrite)
      .save(Config.getLargeTableName("uniform"))

    createMediumTable(spark, Config.getMediumTableName("uniform"), numberOfPartitions)
  }

  def getName: String = "UniformDataGenerator"

  def getMediumTableName: String = Config.getMediumTableName("uniform")

  def getLargeTableName: String = Config.getLargeTableName("uniform")

  case class Key(key: Int)

  case class KeyLabel(key: Int, label: String, pass: Int)

}
