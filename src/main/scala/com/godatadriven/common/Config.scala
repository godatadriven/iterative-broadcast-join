package com.godatadriven.common

import com.typesafe.config.ConfigFactory

object Config {
  private val conf = ConfigFactory.load

  var numberOfBroadcastPasses: Int = conf.getInt("broadcast.passes")

  var broadcastIterationTableName: String = "tmp_broadcast_table.parquet"

  // The number of partitions
  var numberOfPartitions: Int = conf.getInt("generator.partitions")

  // The number of rows
  var numberOfKeys: Int = conf.getInt("generator.keys")

  // The number of times the keys get duplicated,
  // This controls the skewness
  var keysMultiplier: Int = conf.getInt("generator.multiplier")

  def getMediumTableName(generatorType: String): String = {
    conf.getString(s"generator.$generatorType.mediumTableName")
  }

  def getLargeTableName(generatorType: String): String = {
    conf.getString(s"generator.$generatorType.largeTableName")
  }
}