package com.godatadriven

import org.apache.spark.sql.SparkSession


object Entry extends App {

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("Example iterative broadcast join")
    .getOrCreate()

  // Tell Spark to don't be too chatty
  spark.sparkContext.setLogLevel("WARN")

  args.headOption match {
    case Some("generator") => DataGenerator.buildTestset(spark)
    case Some("benchmark") => RunTest.run(spark)
    case _ => println("Not a valid option")
  }
//
//  System.out.println("Work done, press any key to terminate the Spark UI")
//  scala.io.StdIn.readChar()

  spark.stop()
}
