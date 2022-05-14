package org.example.jobs

import org.apache.spark.sql.DataFrame
import org.example.config.SparkSetup.configureDefaultSparkSession
import org.example.kpis.CrimeKPIsCalculation.{calculateCrimeTypeKPIs, calculateLocationCrimesKPIs}

object CrimeKPIs {

  def main(args: Array[String]): Unit = {
    val streetCrimesDataPath = args(0)
    val kpiFor = args(1)
    val outputPath = args(2)
    val spark = configureDefaultSparkSession()

    val data = spark.read.parquet(streetCrimesDataPath)
    val trans_func: DataFrame => DataFrame = if (kpiFor == "crimeType") {
      (data: DataFrame) => calculateCrimeTypeKPIs(data)
    } else if (kpiFor == "district") {
      (data: DataFrame) => calculateLocationCrimesKPIs(data)
    } else {
      throw KpiTypeException("Specified kpi type does not supported")
    }
    val kpis = data.transform(trans_func).cache()
    kpis
      .write
      .json(outputPath)
    kpis.show(1000, truncate = false)
  }

}


final case class KpiTypeException(private val message: String = "",
                                  private val cause: Throwable = None.orNull)
  extends Exception(message, cause)