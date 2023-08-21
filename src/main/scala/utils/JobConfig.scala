package com.gaston.pocs
package utils

import strategies.{DataSourceStrategy, JdbcDataSourceStrategy, BigQueryDataSourceStrategy}

object JobConfig {

  def getBQDatasetName() = "local_weather_info"
  def getTmpGCSBucketName() = "gs://gg-tmp/"

  def getDestinationStrategy(): DataSourceStrategy = {
    val destConfig = if (System.getenv ("DEST").isBlank || System.getenv ("DEST").equalsIgnoreCase ("bigquery") ) new BigQueryDataSourceStrategy()
    else new JdbcDataSourceStrategy("jdbc:mysql://localhost:3306/weather")
    destConfig
  }

  def getInputFile(): String = {
    System.getenv("INPUT_FILE")
  }

}
