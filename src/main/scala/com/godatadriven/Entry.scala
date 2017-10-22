//package com.godatadriven
//
//import com.godatadriven.common.Config
//import org.apache.spark.sql.SparkSession
//
//
//object Entry extends App {
//
//  val appName = s"Join ${Config.joinType}: ${Config.numberOfKeys}"
//
//  println(appName)
//
//  val spark = SparkSession
//    .builder
//    .appName(appName)
//    .getOrCreate()
//
//  spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//  spark.conf.set("spark.kryo.registrationRequired", "true")
//  spark.conf.set("parquet.enable.dictionary", "false")
//  // Tell Spark to don't be too chatty
//  spark.sparkContext.setLogLevel("WARN")
//
//  args.headOption match {
//    case Some("generator") => DataGenerator.buildTestset(spark)
//    case Some("benchmark") => RunTest.run(spark)
//    case _ => println("Not a valid option")
//  }
//
//  spark.stop()
//}
