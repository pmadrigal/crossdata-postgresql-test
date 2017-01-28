package com.stratio.crossdata.connector.postgresql

import com.stratio.common.utils.components.logger.impl.SparkLoggerComponent
import org.apache.spark.sql.crossdata.XDContext
import org.apache.spark.{SparkConf, SparkContext}

object QueryExecutor extends App with SparkLoggerComponent {

  val errorMessage =
    """USAGE:
      |SPARK_HOME/bin/spark-submit --class com.stratio.crossdata.connector.postgresql.QueryExecutor
      |--master "master-url"
      |<path-to-configFile> """.stripMargin

  require(args.length == 1)

  val configFilePath: String = args.head

  val config = new AppConfig(configFilePath)

  val sparkConf = new SparkConf()
    .setAppName("QueryTestExecutor")
    .setAll(config.sparkConfMap)

  val xdContext =  new XDContext(new SparkContext(sparkConf))
  new SqlCompliancePostgresql(xdContext, config.postgresqlURL, config.reportFile)
    .execute()

}
