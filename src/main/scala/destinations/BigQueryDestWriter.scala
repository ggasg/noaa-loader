package com.gaston.pocs
package destinations

import utils.JobConfig
import org.apache.spark.sql.{DataFrame, SaveMode}

class BigQueryDestWriter extends DestWriter {
  override def write(data: DataFrame, destTableName: String, partitionKey: String): Unit = {
    data.write
      .mode(SaveMode.Append)
      .format("bigquery")
      .option("table", JobConfig.getBQDatasetName() + "." + destTableName)
      .save()
  }
}
