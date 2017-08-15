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
    val dataDF: DataFrame = dataProcessor.loadDataFromFile("src/test/resources/data/anime.csv")
  }

  test("Load the series CSV", Tag("loadSeriesCSV")) {
    new fullProcessClass {
      assert(dataDF.count != 0)
    }
  }
}
