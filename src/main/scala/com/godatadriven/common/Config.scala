package com.godatadriven.common

object Config {
  val numberOfPasses = 3

  // The number of partitions
  val numberOfPartitions: Int = 800

  // The number of rows
  val numberOfKeys: Int = Math.pow(10, 7).toInt

  val keysMultiplier: Int = Math.pow(10, 1).toInt

  // The number of rows
  val skewnessExponent: Int = 4

  // Determines the skewness of the keys
  val randomExponent: Int = 4
}