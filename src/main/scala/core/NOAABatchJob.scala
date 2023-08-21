package com.gaston.pocs
package core

import utils.JobConfig
import org.apache.spark.sql.functions.{col, current_timestamp}
import org.apache.spark.sql.{SparkSession}

object NOAABatchJob {
  def main(args: Array[String]): Unit = {
    val destConfig = JobConfig.getDestinationStrategy()

    val inputFile = JobConfig.getInputFile()
    if (inputFile.isBlank) {
      throw new IllegalArgumentException("1 Input Argument Required: <Input file Path>")
    }

    val bucket = "gs://gg-tmp/"

    val spark = SparkSession.builder.appName("NOAA Weather Station Measurements")
      .master("local[1]")
      .config("spark.driver.extraJavaOptions", "- Duser.timezone = UTC")
      .config("spark.executor.extraJavaOptions", "- Duser.timezone = UTC")
      .config("spark.sql.session.timeZone", "UTC")
      .config("temporaryGcsBucket", bucket)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Load NOAA CSV File
    val mainData = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(inputFile)

    // --- Transformations and cleanup ---
    // Station Lookup Table
    val stationLookup = mainData.select("STATION", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION")
      .withColumnRenamed("STATION", "station_id").distinct()
      .withColumnRenamed("NAME", "station_name")
      .withColumnRenamed("LATITUDE", "latitude")
      .withColumnRenamed("LONGITUDE", "longitude")
      .withColumnRenamed("ELEVATION", "elevation")
      .withColumn("last_update", current_timestamp())

    // Weather Measurements from same file
    val stationMeasures = mainData.select("DATE", "STATION", "PRCP", "TMAX", "TMIN", "AWND", "PGTM")
      .withColumnRenamed("STATION", "station_id")
      .withColumnRenamed("DATE", "date").withColumn("date", col("date").cast("date"))
      .withColumnRenamed("PRCP", "precip")
      //      .withColumnRenamed("TAVG", "avg_temp")
      .withColumnRenamed("TMAX", "max_temp")
      .withColumnRenamed("TMIN", "min_temp")
      .withColumnRenamed("AWND", "avg_wind_speed")
      .withColumnRenamed("PGTM", "peak_gust_time")
      .where("date IS NOT NULL")
      .withColumn("last_update", current_timestamp())

    // Write to Destination per selected Strategy
    destConfig.write(stationLookup, "station")
    destConfig.write(stationMeasures, "measures")

    spark.stop()
  }
}
