package com.godatadriven

import co.theasi.plotly._
import com.godatadriven.DataGenerator.generateSkewedSequence
import com.godatadriven.common.Config
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}

object DataGenerator {

  /**
    * Generates a sequence of numbers, for example num = 22 would generate:
    * Array(22, 10, 6, 4, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
    *
    * @param num number of elements in the sequence
    * @return as sequence of numbers
    */
  def generateSkewedSequence(num: Int): List[(Int, Int)] =
    (0 to num).par.map(i =>
      (i, Math.round(
        (num.toDouble - i.toDouble) / (i.toDouble + 1.0)
      ).toInt)
    ).toList

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

  def buildTestset(): Unit = {
    val spark = Utils.getSpark

    def schema: StructType = StructType(
      StructField("key", IntegerType, nullable = false) :: Nil
    )

    import spark.implicits._

    val skewedSeq = generateSkewedSequence(Config.numberOfKeys)

    val skewedSequence =
      (0 until Config.keysMultiplier).flatMap(_ => skewedSeq)

    val rdd = spark
      .sparkContext
      .parallelize(skewedSequence, Config.numberOfPartitions)
      .flatMap(pair => skewDistribution(pair._1, pair._2))
      .map(key => Row(key))

    val dfLarge = spark.createDataFrame(rdd, schema)

    dfLarge
      .write
      .mode(SaveMode.Overwrite)
      .save("table_large.parquet")

    //    println(s"Number of rows: ${dfLarge.count()}")
    //    println(s"Number of keys: ${dfLarge.distinct().count()}")

    // Get an idea of the distribution, by dumping it into csv and load it into plotly
    val dist =
      spark
        .read
        .parquet("table_large.parquet")
        .groupBy("key")
        .count()
        .map(row => row.getAs[Int](0) -> row.getAs[Long](1))
        .rdd
        .takeOrdered(1000)(Ordering[Int].on { x => x._1 })

    val x = dist.map(_._1)
    val y = dist.map(_._2.toDouble)
    val plot = Plot().withScatter(x, y)

    draw(plot, "iterative-broadcast-dist")

    spark
      .read
      .parquet("table_large.parquet")
      .distinct()
      .map(row => (row.getAs[Int](0), s"label-${row.get(0)}", s"description telling something about id ${row.get(0)}"))
      .toDF("key", "label", "description")
      .write
      .mode(SaveMode.Overwrite)
      .save("table_medium.parquet")
  }

}
