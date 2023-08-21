package com.gaston.pocs
package utils

import destinations.{DestWriter, LocalDBDestWriter, BigQueryDestWriter}

object JobConfig {

  def getBQDatasetName() = "local_weather_info"

  def getDestinationStrategy(): DestWriter = {
    val destConfig = if (System.getenv ("DEST").isBlank || System.getenv ("DEST").equalsIgnoreCase ("bigquery") ) new BigQueryDestWriter()
    else new LocalDBDestWriter("jdbc:mysql://localhost:3306/weather")
    destConfig
  }

  def getInputFile(): String = {
    System.getenv("INPUT_FILE")
  }

}
