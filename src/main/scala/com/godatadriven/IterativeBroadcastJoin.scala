package com.godatadriven


object IterativeBroadcastJoin extends App {

  // sbt "run generator"

  args.headOption match {
    case Some("generator") => DataGenerator.buildTestset()
    case Some("benchmark") => RunTest.run()
    case _ => println("Not a valid option")
  }

  System.out.println("Work done, Wait to terminate the Spark UI")
  // To keep the Spark UI alive :-)
  Thread.sleep(1925000)
}
