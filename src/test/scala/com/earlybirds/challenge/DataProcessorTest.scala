package com.earlybirds.challenge

import configuration.DataProcessorConfiguration
import org.apache.spark.sql.DataFrame
import org.scalatest._

class DataProcessorTest extends FunSuite with BeforeAndAfterAll {
  private val conf = new DataProcessorConfiguration

  private val dataProcessor = new DataProcessor(conf)
  private val spark = conf.SparkJob.getSqlContext()

  import spark.implicits._

  override def afterAll(): Unit = {
    spark.stop()
  }

  class fullProcessClass {
    val dataDF: DataFrame = dataProcessor.loadDataFromFile("src/test/resources/data/xag.csv")
  }

  test("Load the series CSV", Tag("loadSeriesCSV")) {
    new fullProcessClass {
      assert(dataDF.count > 0)
      //dataProcessor.createCSVFromDataFrame(dataDF, "src/test/resources/data/agg_ratings.csv")
      dataDF.show(10)
    }
  }

  test("Check the rating sum", Tag("checkRating")) {
    // Check value + Check penality
    // Check groupBy
    // Check sum of similar couple
    // Keeps rating > 0.01
    assert(true)
  }

  test("Check userIdAsInteger", Tag("checkUser")) {
    assert(true)
    // Check if there is not a negative value
    // Check that 0 is present
    // Count the number of different users and check  == the last userIdAsInteger - 1
    // Check that for two different users they dont have the same userIdAsInteger
  }

  test("Check itemIdAsInteger", Tag("checkItem")) {
    // Check if there is not a negative value
    // Check that 0 is present
    // Count the number of different items and check  == the last itemIdAsInteger - 1
    // Check that for two different users they dont have the same itemIdAsInteger
    assert(true)
  }

  test("Check aggRating DF", Tag("checkAggRating")) {
    assert(true)
  }

  test("Check lookupUser DF", Tag("checkLookupUser")) {
    assert(true)
  }

  test("Check lookupProduct DF", Tag("checkLookupProduct")) {
    assert(true)
  }
}
