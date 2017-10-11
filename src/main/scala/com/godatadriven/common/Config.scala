package com.godatadriven.common

object Config {
  // The number of partitions
  val numberOfPartitions: Int = 4000

  // The number of rows
  val numberOfRows: Long = Math.pow(10, 9).toLong

  // The number of keys
  val numberOfKeys: Long = Math.pow(10, 5).toLong

  // Determines the skewness of the keys
  val randomExponent: Int = 4
}