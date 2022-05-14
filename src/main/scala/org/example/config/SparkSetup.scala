package org.example.config

import org.apache.spark.sql.SparkSession

object SparkSetup {
  def configureDefaultSparkSession(): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
  }
}
