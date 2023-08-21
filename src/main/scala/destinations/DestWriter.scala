package com.gaston.pocs
package destinations

import org.apache.spark.sql.DataFrame

trait DestWriter {
  def write(data: DataFrame, destObjName: String, partitionKey: String = null): Unit
}
