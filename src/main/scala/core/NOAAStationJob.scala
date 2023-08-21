package com.gaston.pocs
package core

import utils.JobConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp

object NOAAStationJob {
  def main(args: Array[String]): Unit = {

    val destConfig = JobConfig.getDestinationStrategy()

    val inputFile = JobConfig.getInputFile()
    if (inputFile.isBlank) {
      throw new IllegalArgumentException("1 Input Argument Required: <Input file Path>")
    }
    val spark = SparkSession.builder.appName("NOAA Weather Station Measurements")
      .master("local[1]")
      .config("spark.driver.extraJavaOptions", "- Duser.timezone = UTC")
      .config("spark.executor.extraJavaOptions", "- Duser.timezone = UTC")
      .config("spark.sql.session.timeZone", "UTC")
      .config("temporaryGcsBucket", JobConfig.getTmpGCSBucketName())
      .getOrCreate()

    // Load NOAA CSV File
    val mainData = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(inputFile)

    val stationLookup = mainData.select("STATION", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION")
      .withColumnRenamed("STATION", "station_id").distinct()
      .withColumnRenamed("NAME", "station_name")
      .withColumnRenamed("LATITUDE", "latitude")
      .withColumnRenamed("LONGITUDE", "longitude")
      .withColumnRenamed("ELEVATION", "elevation")
      .withColumn("last_update", current_timestamp())

    destConfig.write(stationLookup, "station")
  }
}
