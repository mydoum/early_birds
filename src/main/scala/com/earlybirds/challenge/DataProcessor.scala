package com.earlybirds.challenge

import org.apache.spark.sql.DataFrame
import configuration.DataProcessorConfiguration
import org.apache.commons.net.ntp.TimeStamp

class DataProcessor(conf: DataProcessorConfiguration) {
  private val spark = conf.SparkJob.getSqlContext()
  import spark.implicits._

  /**
    * Load the data into a DataSet[Row]
    * @param csvPath Data file path
    * @return
    */
  def loadDataFromFile(csvPath: String): DataFrame = {
    spark.read
      .format("csv")
      .schema(conf.dataSchema)
      .option("header", "false")
      .load(csvPath)
  }

  def calculateRatingSum(data: DataFrame): DataFrame = {
    // Find the max timestamp
    // First calculate each rating
    // GroupBy("userId", "colId")
    // Keep rating > 0.01
    spark.emptyDataFrame
  }

  /**
    *
    * @param actualTimeStamp
    * @param maxTimeStamp
    * @return
    */
  def diffDaysBetweenTimestamps(actualTimeStamp: TimeStamp, maxTimeStamp: TimeStamp): Int = {
    0
  }

  // Combine the two transform data
  def transformItemIDToInteger(data: DataFrame): DataFrame = {
    spark.emptyDataFrame
  }

  def transformUserIDToInteger(data: DataFrame): DataFrame = {
    spark.emptyDataFrame
  }

  def createAggRating(data: DataFrame): DataFrame = {
    // Select('userIdAsInteger, 'itemIdAsInteger, 'ratingSum)
    spark.emptyDataFrame
  }

  def createLookupUser(data: DataFrame): DataFrame = {
    // Select('userId, 'userIdAsInteger)
    spark.emptyDataFrame
  }

  def createLookupProduct(data: DataFrame): DataFrame = {
    // Select('itemId, 'itemIdAsInteger)
    spark.emptyDataFrame
  }

  def createCSVFromDataFrame(data: DataFrame, path: String): Unit =
    data
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(path)
}
