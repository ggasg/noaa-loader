package com.gaston.pocs
package strategies

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataSourceStrategy {
  def write(data: DataFrame, destObjName: String, partitionKey: String = null): Unit
  def read(sparkSession: SparkSession, tableName: String): DataFrame
}
