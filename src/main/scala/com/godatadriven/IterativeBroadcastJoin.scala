package com.godatadriven


object IterativeBroadcastJoin extends App {

  // sbt "run generate"

  args.headOption match {
    case Some("generator") => DataGenerator.buildTestset()
    case Some("benchmark") => RunTest.run()
    case _ => print("Not a valid option")
  }
  // To keep the Spark UI alive :-)
  Thread.sleep(1925000)
}
