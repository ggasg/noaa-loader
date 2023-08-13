package com.gaston.pocs

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{SaveMode, SparkSession}

object StationDetails {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      throw new IllegalArgumentException("1 Input Argument Required: <Input file Path>")
    }

    val bucket = "gg-tmp"
    val bucketOutput = "gs://gg-weather-sources/measurements.parquet"

    val spark = SparkSession.builder.appName("Station Lookup Table")
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
      .load(args(0))


    // Station Lookup Table
    mainData.select("STATION", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION")
      .withColumnRenamed("STATION", "station_id").distinct()
      .withColumnRenamed("NAME", "station_name")
      .withColumnRenamed("LATITUDE", "latitude")
      .withColumnRenamed("LONGITUDE", "longitude")
      .withColumnRenamed("ELEVATION", "elevation")
      .withColumn("last_update", current_timestamp())
      .write
      .mode(SaveMode.Append)
      .format("bigquery")
      .option("table", "local_weather_info.station")
      .save()

    // Weather Measurements from same file
    mainData.select("DATE", "STATION", "PRCP", "TMAX", "TMIN", "AWND", "PGTM")
      .withColumnRenamed("STATION", "station_id")
      .withColumnRenamed("DATE", "date").withColumn("date", col("date").cast("date"))
      .withColumnRenamed("PRCP", "precip")
//      .withColumnRenamed("TAVG", "avg_temp")
      .withColumnRenamed("TMAX", "max_temp")
      .withColumnRenamed("TMIN", "min_temp")
      .withColumnRenamed("AWND", "avg_wind_speed")
      .withColumnRenamed("PGTM", "peak_gust_time")
      .withColumn("last_update", current_timestamp())
      .write
      .mode(SaveMode.Append)
      .format("bigquery")
      .option("table", "local_weather_info.measurements")
      .save()

    // Just testing a parquet output
//    mainData.write.partitionBy("DATE").parquet(bucketOutput)

    spark.stop()
  }
}
