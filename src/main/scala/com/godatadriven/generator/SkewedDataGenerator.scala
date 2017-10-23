package com.godatadriven.generator

import com.godatadriven.common.Config
import org.apache.spark.sql.{SaveMode, SparkSession}

object SkewedDataGenerator extends DataGenerator {

  def buildTestset(spark: SparkSession,
                   numberOfKeys: Int = Config.numberOfKeys,
                   keysMultiplier: Int = Config.keysMultiplier,
                   numberOfPartitions: Int = Config.numberOfPartitions): Unit = {

    import spark.implicits._

    println(s"Generating ${numberOfRows(numberOfKeys, keysMultiplier)} rows")

    val df = spark
      .sparkContext
      .parallelize(generateSkewedSequence(numberOfKeys), numberOfPartitions)
      .flatMap(list => (0 until keysMultiplier).map(_ => list))
      .repartition(numberOfPartitions)
      .flatMap(pair => skewDistribution(pair._1, pair._2))
      .toDS()
      .map(Key)
      .repartition(numberOfPartitions)

    assert(df.count() == numberOfRows())

    df
      .write
      .mode(SaveMode.Overwrite)
      .save(Config.getLargeTableName("skewed"))

    createMediumTable(spark, Config.getMediumTableName("skewed"), numberOfPartitions)
  }

  /**
    * Will generate a sequence of the input sample
    *
    * @param key   The sample and the
    * @param count count the number of repetitions
    * @return
    */
  def skewDistribution(key: Int, count: Int): Seq[Int] = Seq.fill(count)(key)

  def getName: String = "SkewedDataGenerator"

  def getMediumTableName: String = Config.getMediumTableName("skewed")

  def getLargeTableName: String = Config.getLargeTableName("skewed")

  case class Key(key: Int)

  case class KeyLabel(key: Int, label: String, pass: Int)

}
