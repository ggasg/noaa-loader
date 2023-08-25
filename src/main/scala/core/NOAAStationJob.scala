package com.gaston.pocs
package core

import utils.{JobConfig, JobUtils}

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object NOAAStationJob {
  def main(args: Array[String]): Unit = {

    val destConfig = JobConfig.getDestinationStrategy()

    val inputFile = JobConfig.getInputFile()
    if (inputFile.isBlank) {
      throw new IllegalArgumentException("1 Input Argument Required: <Input file Path>")
    }
    val spark = SparkSession.builder.appName("NOAA Weather Station Lookup Records")
      .master("local[1]")
      .config("spark.driver.extraJavaOptions", "- Duser.timezone = UTC")
      .config("spark.executor.extraJavaOptions", "- Duser.timezone = UTC")
      .config("spark.sql.session.timeZone", "UTC")
      .config("temporaryGcsBucket", JobConfig.getTmpGCSBucketName())
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Load NOAA CSV File
    val mainData = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(inputFile)

    val currentStations = destConfig.read(spark, "station").select("station_id")

    // This UDF generates an integer from the given string Station ID
    import spark.implicits._
    val getUniqueStationId = functions.udf(JobUtils.generateIntegerKey(_))

    // Since we're appending, let's load only the stations we haven't loaded so far
    val stationLookup = mainData.select("STATION", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION")
      .withColumnRenamed("STATION", "station_code").distinct()
      .withColumn("station_id", getUniqueStationId($"station_code"))
      .withColumnRenamed("NAME", "station_name")
      .withColumnRenamed("LATITUDE", "latitude")
      .withColumnRenamed("LONGITUDE", "longitude")
      .withColumnRenamed("ELEVATION", "elevation")
      .withColumn("last_update", current_timestamp())
      .join(currentStations, Seq("station_id"), "leftouter")
      .where(isnull(currentStations("station_id")))

    destConfig.write(stationLookup, "station")
  }
}
