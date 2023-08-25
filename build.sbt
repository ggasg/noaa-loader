ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

val gcsVersion = "hadoop3-2.2.12"

lazy val root = (project in file("."))
  .settings(
    name := "noaa-loader",
    idePackagePrefix := Some("com.gaston.pocs")
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.2"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"

libraryDependencies ++= Seq("com.google.cloud.bigdataoss" % "gcs-connector" % gcsVersion,
  "com.google.cloud" % "google-cloud-storage" % "0.7.0",
  "com.google.cloud.spark" % "spark-bigquery-with-dependencies_2.13" % "0.32.2"
)

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "org.scalameta" %% "munit" % "0.7.26" % Test