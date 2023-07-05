ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "sparkmls"
  )

libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.4.1"
