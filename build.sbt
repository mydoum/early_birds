organization := "com.earlybirds.challenge"

name := "challenge"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"
val sparkDependencyScope = "provided"

fork := true

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyScope
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

