name := "analyze"

version := "1.0"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/")

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.0"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.2.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.4"

parallelExecution in Test := false