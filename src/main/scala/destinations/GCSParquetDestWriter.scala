package com.gaston.pocs
package destinations
import org.apache.spark.sql.DataFrame

class GCSParquetDestWriter(bucketName: String) extends DestWriter {
  override def write(data: DataFrame, destFile: String, partitionKey: String): Unit = {
    val bucketOutput = bucketName + destFile

    data.write.partitionBy(partitionKey).parquet(bucketOutput)
  }
}
