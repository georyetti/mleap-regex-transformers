import sbt.Keys._

ThisBuild / organization := "georyetti"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version := "0.0.1"

val sparkVersion = "3.2.2"
val mleapVersion = "0.21.0"


val mainClassName = "Sample"

Compile / run / mainClass := Some(mainClassName) // for the main 'sbt run' task

val dependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "ml.combust.mleap" %% "mleap-runtime" % mleapVersion,
  "ml.combust.mleap" %% "mleap-spark-base" % mleapVersion,
  "org.scalatest" %% "scalatest-funspec" % "3.2.14" % "test",
)


lazy val root = (project in file("."))
  .settings(
    name := "mleap-regex-transformers",
    libraryDependencies ++= dependencies,
    run / fork := true,
    run / javaOptions ++= Seq("-Dspark.master=local[*]")
  )