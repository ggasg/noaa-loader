package com.gaston.pocs
package strategies

import utils.JobConfig

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class BigQueryDataSourceStrategy extends DataSourceStrategy {
  override def write(data: DataFrame, destTableName: String, partitionKey: String): Unit = {
    data.write
      .mode(SaveMode.Append)
      .format("bigquery")
      .option("table", JobConfig.getBQDatasetName() + "." + destTableName)
      .save()
  }

  override def read(sparkSession: SparkSession, tableName: String): DataFrame = ???
}
