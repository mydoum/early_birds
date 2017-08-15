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
    val seriesDF: DataFrame = rankingProcessor.getSeriesDataFrameFromFile("src/test/resources/data/anime.csv")
    val filteredDF: DataFrame = rankingProcessor.filterAnimeDF(seriesDF)
    val transform: DataFrame = rankingProcessor.transformGenreStringToArray(filteredDF)
    val getGenre: DataFrame = rankingProcessor.explodeDataFrameByGenre(transform)
    val reduceGenre: DataFrame = rankingProcessor.reduceRatingByGenre(getGenre)
    val sortedResult: DataFrame = rankingProcessor.sortByRating(reduceGenre)
  }

  test("Load the series CSV", Tag("loadSeriesCSV")) {
    new fullProcessClass {
      assert(seriesDF.count != 0)
    }
  }
}
