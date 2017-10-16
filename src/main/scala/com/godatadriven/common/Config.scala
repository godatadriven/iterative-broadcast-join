package com.godatadriven.common

import com.typesafe.config.{ConfigFactory}

object Config {
  private val conf = ConfigFactory.load

  val broadcastIterations: Int = conf.getInt("join.broadcast.iterations")

  // The number of partitions
  val numberOfPartitions: Int = conf.getInt("generator.partitions")

  // The number of rows
  val numberOfKeys: Int = conf.getInt("generator.keys")

  // The number of times the keys get duplicated,
  // This controls the skewness
  val keysMultiplier: Int = conf.getInt("generator.multiplier")

  val joinType: String = conf.getString("join.type")
}