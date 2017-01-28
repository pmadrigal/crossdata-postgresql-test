package com.stratio.crossdata.connector.postgresql

import java.io.File

import com.stratio.crossdata.connector.postgresql.QueryExecutor.logger
import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConversions._

class AppConfig(fileName: String) {

  private def getAppConfig(configFileName: String): Config = {
    val configFile = new File(configFileName)
    if (configFile.exists()) logger.info("file uploaded correctly")
    else logger.error(s"File $configFileName doesn't exist")
    ConfigFactory.parseFile(configFile)
  }

  private val config: Config = getAppConfig(fileName)

  val sparkConfMap: Map[String, String] =
    config.getConfig("spark")
      .entrySet()
      .map(entry => entry.getKey -> entry.getValue.unwrapped().toString)
      .toMap
  val postgresqlURL: String = config.getConfig("postgresql").getString("url")
  val reportFile: String = config.getString("reportfile")

}
