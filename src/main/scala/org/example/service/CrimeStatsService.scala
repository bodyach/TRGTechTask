package org.example.service

import com.twitter.finagle.{http, Http, Service}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.{Await, Future}
import com.twitter.util.logging.Logger
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.DataFrame
import org.example.config.SparkSetup.configureDefaultSparkSession
import org.example.kpis.CrimeKPIsCalculation.{calculateCrimeTypeKPIs, calculateLocationCrimesKPIs}

object CrimeStatsService extends App {

  lazy val log = Logger.getLogger(CrimeStatsService.getClass.getName)
  val TableName = "streetCrimes"

  val spark = configureDefaultSparkSession()

  val crimeDataPath = args(0)

  val streetCrimesTable = {
    spark.read
      .parquet(crimeDataPath)
      .withColumn("index", monotonically_increasing_id())
      .createOrReplaceTempView(TableName)
    spark.sql(s"CACHE TABLE $TableName")
    spark.table(TableName)
  }

  def buildJsonResponse(request: Request, data_trans_func: () => DataFrame): Response = {
    val response = Response(request.version, http.Status.Ok)
    response.setContentType("application/json", "UTF-8")
    val data = data_trans_func()
      .toJSON
      .collect()
      .mkString("\n")
    response.setContentString(data)
    response
  }

  def serveRequest(request: Request): Response = {
    request.path match {
      case "/" =>
        val query = Option(request.getParam("query"))
        query match {
          case Some(query) =>
            val sqlQueryFunc = () => spark.sql(query)

            buildJsonResponse(request, sqlQueryFunc)
          case None => Response(request.version, http.Status.BadRequest)
        }

      case "/crimesList" =>
        val lowerIndex = Option(request.getParam("lowerIndex")).getOrElse(0)
        val upperIndex = Option(request.getParam("upperIndex")).getOrElse(1000)
        val getDataRangeFunc = () => streetCrimesTable
          .where(s"index BETWEEN $lowerIndex AND $upperIndex")
          .drop("index")

        buildJsonResponse(request, getDataRangeFunc)

      case "/kpis/crimes" =>
        val crimesTypeFunc = () => streetCrimesTable
          .transform(calculateCrimeTypeKPIs)
          .cache()

        buildJsonResponse(request, crimesTypeFunc)

      case "/kpis/locations" =>
        val districtCrimesFunc = () => streetCrimesTable
          .transform(calculateLocationCrimesKPIs)
          .cache()

        buildJsonResponse(request, districtCrimesFunc)

      case _: Any => Response(request.version, http.Status.BadRequest)
    }
  }

  def loadRequestedDataRange(request: Request): Array[String] = {
    val lowerIndex = Option(request.getParam("lowerIndex")).getOrElse(0)
    val upperIndex = Option(request.getParam("upperIndex")).getOrElse(1000)
    streetCrimesTable
      .where(s"index BETWEEN $lowerIndex AND $upperIndex")
      .drop("index")
      .toJSON
      .collect()
  }

  def initHTTPServer(): Unit = {
    val service = new Service[http.Request, http.Response] {
      def apply(req: http.Request): Future[http.Response] = {
        val response = serveRequest(req)
        Future.value(response)
      }
    }
    val server = Http.serve(":10000", service)
    Await.ready(server)
  }

  initHTTPServer()
}
