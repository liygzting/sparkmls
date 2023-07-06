ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "sparkmls"
  )

//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1"
//libraryDependencies += "org.apache.spark" %% "spark-sql_2.12" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.4.1"
libraryDependencies += "org.apache.sedona" %% "sedona-sql-3.4" % "1.4.1"