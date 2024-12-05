name := "CountBy"

version := "1.0.5"

scalaVersion := "2.12.17"

val sparkVersion = "3.4.0"
val circeVersion = "0.13.0"
val hbaseVersion = "2.5.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-avro" % sparkVersion

libraryDependencies += "org.apache.hbase" % "hbase-shaded-client" % hbaseVersion

libraryDependencies += "io.circe" %% "circe-generic" % circeVersion
libraryDependencies += "io.circe" %% "circe-optics" % circeVersion
libraryDependencies += "io.circe" %% "circe-parser" % circeVersion

mainClass := Some("it.acsoftware.hyperiot.spark.countby.CountBy")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}