name := "analyze"

version := "1.0"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Spring repository" at "http://repo.springsource.org/libs-release-remote/")

resolvers += Resolver.sonatypeRepo("public")

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.0-cdh5.3.0"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.2.0-cdh5.3.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.4"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

parallelExecution in Test := false

mainClass in assembly := Some("edu.nd.dsg.bshi.Main")

test in assembly := {}

val meta = """META.INF(.)*""".r

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case meta(_) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}