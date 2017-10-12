package com.godatadriven.common

object Config {
  // The number of partitions
  val numberOfPartitions: Int = 4000

  // The number of rows
  val numberOfKeys: Int = Math.pow(10, 7).toInt

  // The number of rows
  val skewnessExponent: Int = 4

  // Determines the skewness of the keys
  val randomExponent: Int = 4
}