package org.example.jobs

import org.example.config.SparkSetup.configureDefaultSparkSession

object ReadParquetInputData {

  def main(args: Array[String]): Unit = {
    val spark = configureDefaultSparkSession()

    val inputDataPath = args(0)
    val numbersOfRowsToShow = args(1).toInt
    val tableName = args(2)
    val sqlToExecute = args(3)

    if (sqlToExecute == "NOSQL") {
      spark.read.parquet(inputDataPath).show(numbersOfRowsToShow, truncate = false)
    } else {
      spark.read.parquet(inputDataPath).createOrReplaceTempView(tableName)
      spark.sql(sqlToExecute).show(numbersOfRowsToShow, truncate = false)
    }
  }
}
