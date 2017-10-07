package com.godatadriven.common

object Config {
  // The number of partitions
  val numberOfPartitions: Int = 200

  // The number of distinct key (upper bound)
  val numberOfKeys: Int = Math.pow(10, 5).toInt

  // The number of rows
  val numberOfRows: Int = Math.pow(10, 10).toInt

  // Determines the skewness of the keys
  val randomExponent = 19.25
}