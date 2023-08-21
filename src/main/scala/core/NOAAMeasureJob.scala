package com.gaston.pocs
package core

import utils.JobConfig

import org.apache.spark.sql.functions.{col, current_timestamp, max}
import org.apache.spark.sql.SparkSession

object NOAAMeasureJob {
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

    spark.sparkContext.setLogLevel("ERROR")

    // Load NOAA CSV File
    val mainData = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(inputFile)

    // Get checkpoint for a differential load based on the last date of measure
    val latestDate = destConfig.read(spark, "measures")
      .groupBy()
      .agg(max("date"))
      .first()
      .getDate(0)

    // --- Transformations and cleanup ---
    // Weather Measurements
    val stationMeasures = mainData.select("DATE", "STATION", "PRCP", "TMAX", "TMIN", "AWND", "PGTM")
      .withColumnRenamed("STATION", "station_id")
      .withColumnRenamed("DATE", "date").withColumn("date", col("date").cast("date"))
      .withColumnRenamed("PRCP", "precip")
      .withColumnRenamed("TAVG", "avg_temp")
      .withColumnRenamed("TMAX", "max_temp")
      .withColumnRenamed("TMIN", "min_temp")
      .withColumnRenamed("AWND", "avg_wind_speed")
      .withColumnRenamed("PGTM", "peak_gust_time")
      .where("date IS NOT NULL")
      .where(mainData.col("date") > latestDate)
      .withColumn("last_update", current_timestamp())

    // Write to Destination per selected Strategy
    destConfig.write(stationMeasures, "measures")

    spark.stop()
  }
}
