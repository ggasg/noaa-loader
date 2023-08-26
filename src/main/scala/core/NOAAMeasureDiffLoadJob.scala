package com.gaston.pocs
package core

import utils.{JobConfig, JobUtils}

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}

object NOAAMeasureDiffLoadJob {
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

//    spark.sparkContext.setLogLevel("ERROR")
    val logger = Logger.getLogger(this.getClass.getName)

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

    val validStationIds = destConfig.read(spark, "station").select("station_id")

    // This UDF generates an integer from the given string Station ID
    import spark.implicits._
    val getUniqueStationId = functions.udf(JobUtils.generateIntegerKey(_))

    // --- Transformations and cleanup ---
    // Delta of Valid Measurements for dates not present in the DB. Requirement is to never re-process dates in this job
    val validMeasures = mainData.select("STATION","DATE", "PRCP", "TMAX", "TMIN", "AWND", "PGTM")
      .withColumn("station_id", getUniqueStationId($"STATION"))
      .withColumnRenamed("DATE", "date").withColumn("date", col("date").cast("date"))
      .withColumnRenamed("PRCP", "precip")
      .withColumnRenamed("TAVG", "avg_temp")
      .withColumnRenamed("TMAX", "max_temp")
      .withColumnRenamed("TMIN", "min_temp")
      .withColumnRenamed("AWND", "avg_wind_speed")
      .withColumnRenamed("PGTM", "peak_gust_time")
      .where(col("date") > (if (latestDate != null) latestDate else "1899-01-01"))
      .withColumn("last_update", current_timestamp())
      .drop("STATION")

    // We also separate the records we couldn't lookup against station table, this way the job doesn't fail
    val measuresValidStations = validMeasures
      .join(validStationIds, Seq("station_id"), "leftouter")
      .where(!isnull(validStationIds("station_id")))

    // Invalid records go somewhere else
    val measuresLookupFailed = validMeasures
      .withColumn("error_cause", lit("Station Lookup Failed"))
      .join(validStationIds, Seq("station_id"), "leftouter")
      .where(isnull(validStationIds("station_id")))

    // Pre-aggregated dataset by Month of Year (Averages, Totals, Maxes, etc)
    val aggregatesPerMoY = measuresValidStations.select("date", "precip")
      .withColumn("month_of_year", date_format(col("date"), "y-M"))
      .groupBy("month_of_year")
      .agg(countDistinct(when(col("precip") > 0, col("date")).otherwise(null)).as("rainy"),
        countDistinct("date").as("no_days"))
      .withColumn("pct_rainy_days", round(($"rainy" / $"no_days") * 100, 2))
      .drop("precip", "date", "rainy", "no_days")

    destConfig.write(measuresValidStations, "measures")

    destConfig.write(measuresLookupFailed, "measure_errors")
    val numErrors = measuresLookupFailed.count()
    // TODO - How do we generate a good logging format? Is it necessary when running in GCP?
    if (numErrors > 0) logger.info(s"noaa measures job: Wrote $numErrors records in measure_errors")

    destConfig.write(aggregatesPerMoY, "measure_aggregates")

    spark.stop()
  }
}
