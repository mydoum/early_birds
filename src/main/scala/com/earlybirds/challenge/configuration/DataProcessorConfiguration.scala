package com.earlybirds.challenge.configuration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class DataProcessorConfiguration {
  private val user_id   = StructField("user_id",    DataTypes.StringType)
  private val item_id   = StructField("item_id",    DataTypes.StringType)
  private val rating    = StructField("rating",     DataTypes.FloatType)
  private val timestamp = StructField("timestamp",  DataTypes.TimestampType)

  private val fields = Array(user_id, item_id, rating, timestamp)

  val dataSchema = StructType(fields)

  object SparkJob {
    private val sqlContext = SparkSession
      .builder
      .appName("User data processor")
      .master("local[*]")
      .getOrCreate()

    sqlContext.sparkContext.setLogLevel("ERROR")

    def getSqlContext() = {
      sqlContext
    }
  }

}
