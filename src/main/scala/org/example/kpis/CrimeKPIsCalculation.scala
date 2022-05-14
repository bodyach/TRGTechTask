package org.example.kpis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, count, dense_rank, desc, round}

object CrimeKPIsCalculation {

  def calculateCrimeTypeKPIs(data: DataFrame): DataFrame = {
    val crimesCount = data
      .groupBy("crimeType")
      .agg(count("*").as("overall"))
    val crimeDistrictLeader = data
      .groupBy("crimeType", "districtName")
      .agg(count("*").as("district_count"))
      .withColumn("rank", dense_rank().over(Window.partitionBy("crimeType")
        .orderBy(desc("district_count"))))
      .where("rank == 1")
      .drop("rank")
    crimesCount.join(crimeDistrictLeader, "crimeType")
      .withColumn("district_crime_percentage", round(col("district_count") / col("overall") * 100, 2))
      .select(col("crimeType"),
        col("overall"),
        col("districtName").as("most_affected_district"),
        col("district_crime_percentage")
      )
  }

  def calculateLocationCrimesKPIs(data: DataFrame): DataFrame = {
    val locationCrimesCount = data
      .groupBy("districtName")
      .agg(count("*").as("overall"))

    val crimesEdges = data
      .groupBy("crimeType", "districtName")
      .agg(count("*").as("district_count"))
      .withColumn("rank_desc", dense_rank().over(Window.partitionBy("districtName").orderBy(desc("district_count"))))
      .withColumn("rank_asc", dense_rank().over(Window.partitionBy("districtName").orderBy(asc("district_count"))))
      .cache()

    val mostPopularCrime = crimesEdges.where("rank_desc == 1")
    val leastPopularCrime = crimesEdges.where("rank_asc == 1")
    locationCrimesCount
      .join(mostPopularCrime.as("most"), "districtName")
      .join(leastPopularCrime.as("least"), "districtName")
      .select(
        col("districtName"),
        col("overall"),
        col("most.crimeType").as("mostPopularCrime"),
        col("least.crimeType").as("leastPopularCrime")
      )
  }

}
