package org.example.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.example.config.SparkSetup.configureDefaultSparkSession

object CrimeTableCreation {

  def main(args: Array[String]): Unit = {
    val spark = configureDefaultSparkSession()

    val inputPath = args(0)
    val outputPath = args(1)

    val streetCrime = spark.read.option("header", "true")
      .csv(inputPath + "/*/*street*.csv")
      .transform(addDistrictName)
      .transform(renameCrimeColumns)
      .transform(renameLastOutcomeCategory)

    val outcome = spark.read.option("header", "true")
      .csv(inputPath + "/*/*outcomes*.csv")
      .transform(addDistrictName)
      .transform(renameCrimeColumns)
      .transform(renameOutputType)

    val crimesSummary = streetCrime
      .join(outcome,
        streetCrime("crimeId") === outcome("crimeId"), "left")
      .select(
        streetCrime("crimeId"),
        streetCrime("districtName"),
        streetCrime("Latitude"),
        streetCrime("Longitude"),
        streetCrime("crimeType"),
        coalesce(outcome("outcomeType"), streetCrime("lastOutcomeCategory")).as("lastOutcome")
      )

    crimesSummary
      .write
      .parquet(outputPath)
  }

  def addDistrictName(data: DataFrame): DataFrame = {
    data
      .withColumn("input_file", input_file_name())
      .withColumn("districtString", regexp_extract(col("input_file"), "[0-9]{4}-[0-9]{2}-(.*)-", 1))
      .withColumn("districtName", regexp_replace(col("districtString"), "-", " "))
  }

  def renameCrimeColumns(data: DataFrame): DataFrame = {
    data
      .withColumnRenamed("Crime ID", "crimeId")
      .withColumnRenamed("Crime type", "crimeType")
  }

  def renameLastOutcomeCategory(data: DataFrame): DataFrame = {
    data
      .withColumnRenamed("Last outcome category", "lastOutcomeCategory")
  }

  def renameOutputType(data: DataFrame): DataFrame = {
    data
      .withColumnRenamed("Outcome type", "outcomeType")
  }
}
