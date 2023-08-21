package com.gaston.pocs
package destinations
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

class LocalDBDestWriter(localDBUrl: String) extends DestWriter {

  override def write(data: DataFrame, destTableName: String, partitionKey: String): Unit = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", System.getenv("MYSQL_USER"))
    connectionProperties.put("password", System.getenv("MYSQL_PASS"))
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

     data.write.mode(SaveMode.ErrorIfExists)
      .jdbc(localDBUrl, destTableName, connectionProperties)
  }
}
