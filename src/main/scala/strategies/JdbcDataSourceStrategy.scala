package com.gaston.pocs
package strategies
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

class JdbcDataSourceStrategy(localDBUrl: String) extends DataSourceStrategy {
  val connectionProperties = new Properties()
  connectionProperties.put("url", localDBUrl)
  connectionProperties.put("user", System.getenv("MYSQL_USER"))
  connectionProperties.put("password", System.getenv("MYSQL_PASS"))
  connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

  override def write(data: DataFrame, destTableName: String, partitionKey: String): Unit = {
     data.write.mode(SaveMode.Append)
      .jdbc(connectionProperties.getProperty("url"), destTableName, connectionProperties)
  }

  def read(sparkSession: SparkSession, tableName: String): DataFrame = {
    sparkSession.read
      .jdbc(connectionProperties.getProperty("url"), tableName, connectionProperties)
  }
}
