package com.earlybirds.challenge

import configuration.DataProcessorConfiguration

class DataProcessor(conf: DataProcessorConfiguration) {
  private val spark = conf.SparkJob.getSqlContext()
  import spark.implicits._

  def loadDataFromFile(csvPath: String): DataFrame = {
    spark.read
      .format("csv")
      .schema(conf.dataSchema)
      .option("header", "false")
      .load(csvPath)
  }
}
