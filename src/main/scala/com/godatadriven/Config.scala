package com.godatadriven

object Config {
  // The number of partitions
  val numberOfPartitions: Int = 200

  // The number of distinct key (upper bound)
  val numberOfKeys: Int = Math.pow(10, 4).toInt

  // The number of rows
  val numberOfRows: Int = Math.pow(10, 9).toInt

  // Determines the skewness of the keys
  val randomExponent = 22.00
}