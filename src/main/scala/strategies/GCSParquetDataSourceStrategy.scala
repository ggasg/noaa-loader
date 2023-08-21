package com.gaston.pocs
package strategies
import org.apache.spark.sql.{DataFrame, SparkSession}

class GCSParquetDataSourceStrategy(bucketName: String) extends DataSourceStrategy {
  override def write(data: DataFrame, destFile: String, partitionKey: String): Unit = {
    val bucketOutput = bucketName + destFile

    data.write.partitionBy(partitionKey).parquet(bucketOutput)
  }

  override def read(sparkSession: SparkSession, tableName: String): DataFrame = ???
}
