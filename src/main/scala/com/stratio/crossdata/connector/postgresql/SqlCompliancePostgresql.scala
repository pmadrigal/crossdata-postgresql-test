package com.stratio.crossdata.connector.postgresql

import java.io.{File, PrintWriter}

import com.stratio.crossdata.util.using
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.crossdata.{ExecutionType, XDContext, XDDataFrame}
import org.apache.spark.sql.execution.datasources.jdbc.PostgresqlUtils

class SqlCompliancePostgresql(xdContext: XDContext, postgresqlURL: String, reportFile: String) {

  import SqlCompliancePostgresql._

  implicit def dataFrameToXDDataFrame(dataFrame: DataFrame): XDDataFrame = dataFrame match {
    case xdDataFrame: XDDataFrame => xdDataFrame
    case _ =>  new XDDataFrame(dataFrame.sqlContext, dataFrame.queryExecution.logical)
  }
  private lazy val sql = xdContext.sql _

  private def importTables() = {
    val importCommand = "IMPORT TABLES"
    val provider = "com.stratio.crossdata.connector.postgresql"
    val options = Map ("url" -> postgresqlURL, "schema" -> "public").map {case (k, v) => s"$k " + s"'$v'"} mkString ","
    val importSentence = s"$importCommand USING $provider" + options.headOption.fold ("") (_ => s" OPTIONS ( $options ) ")
    sql(importSentence).show
  }

  private object ActionType extends Enumeration{
    type ActionType = Value
    val Collect, Native, Postgresql = Value
  }

  private case class QueryReport(id: Int, execuctionType: ActionType.ActionType, isCorrect: Boolean, time: Long)

  private def resultQueryToRow(value: QueryReport): String = anyToElement(value.id) +
    anyToElement(value.execuctionType.toString) + anyToElement(value.isCorrect) + anyToElement(value.time + " ms")

  private def anyToElement(value: Any): String = {
    val color = value match {
      case b: Boolean if !b => s""" bgcolor="red""""
      case _ => ""
    }
    s"<td$color>" + value.toString + "</td>"
  }

  private def withWriterDo[T](f: (PrintWriter) => T): T = {
    val report = reportFile
    using (new PrintWriter(new File(report))) { writer =>
      f(writer)
    }
  }

  private def tableHeader(name: String, writer: PrintWriter) = {
    writer.write(s"""<p><font size="12">$name</font></p>""")
    writer.write("""<table style=\"width:100%\">""")
    writer.write("""<tr><th>Query ID</th><th>Execution Type</th><th>Result</th><th>Time</th></tr>""")
  }

  private def executeQueriesList(queriesList: List[(Int, String)]) = queriesList.flatMap{
    case (id, query) => List(
      executeQuery(id, query, ActionType.Collect),
      executeQuery(id, query, ActionType.Native),
      executeQuery(id, query, ActionType.Postgresql))
  }
  private def executeQuery(id: Int, query: String, actionType: ActionType.ActionType): QueryReport ={
    try{
      val t0 = System.currentTimeMillis()
      actionType match {
        case ActionType.Collect => sql(query).collect()
        case ActionType.Native => sql(query).collect(ExecutionType.Native)
        case ActionType.Postgresql =>
          PostgresqlUtils.withClientDo(Map("url" -> postgresqlURL)){ (_, stat) =>
            stat.executeQuery(query)
          }
      }
      val t1 = System.currentTimeMillis()
      QueryReport(id, actionType, true, t1-t0)
    }
    catch {
      case e: Exception =>
        println(s"$query with id $id failed in action: $actionType\n ${e.getMessage}")
        QueryReport(id, actionType, false, 0)
    }
  }

  private def writeQueriesResult(queryType: String, queryList: List[(Int, String)], writer: PrintWriter) = {
    tableHeader(queryType, writer)
    executeQueriesList(queryList).foreach{ result =>
      val line = resultQueryToRow(result)
      writer.append("<tr>" + line  + "</tr>")
    }
    writer.append("""</table>""")
  }

  def execute() = {
    importTables()

    withWriterDo { writer =>
      writeQueriesResult("Projections", projectionQueries, writer)
      writeQueriesResult("Filters", filterQueries, writer)
      writeQueriesResult("Aggregation", aggregationQueries, writer)
      writeQueriesResult("Sort", sortQueries, writer)
      writeQueriesResult("Subquery", subqueryQueries, writer)
      writeQueriesResult("Join", joinQueries, writer)
    }
  }
}

object SqlCompliancePostgresql {
  /***PROJECTION***/
  val projectionQueries = List(
    (1, "SELECT * FROM public.person"),
    (2, "SELECT id FROM public.person"),
    (3, "SELECT id as identifier FROM public.person"),
    (4, "SELECT Count(id) FROM public.person"),
    (5, "SELECT Count(id) as identifier FROM public.person"),
    (6, "SELECT CAST(id AS float) as casting FROM public.person"),
    (7, "SELECT Distinct city, name FROM public.person")
  )
  /*** FILTERS***/
  val filterQueries = List(
    (1, "SELECT * FROM public.person, public.school"),
    (2, "SELECT * FROM public.person WHERE id>2"),
    (3, "SELECT * FROM public.person WHERE id>=2"),
    (4, "SELECT * FROM public.person WHERE id<2"),
    (5, "SELECT * FROM public.person WHERE id<=2"),
    (6, "SELECT * FROM public.person WHERE id BETWEEN 1 AND 5"),
    (7, "SELECT * FROM public.person WHERE id NOT BETWEEN 1 AND 5"),
    (8, "SELECT * FROM public.person WHERE id IN (1,2)"),
    (9, "SELECT * FROM public.person WHERE id NOT IN (1,2)"),
    (10, "SELECT * FROM public.person WHERE name LIKE '%1'"),
    (11, "SELECT * FROM public.person WHERE name LIKE '%a%'"),
    (12, "SELECT * FROM public.person WHERE name LIKE 'n%'"),
    (13, "SELECT * FROM public.person WHERE id=1 AND name LIKE '%a%'"),
    (14, "SELECT * FROM public.person WHERE id=1 OR name LIKE '%a%'"),
    (15, "SELECT * FROM public.person WHERE (id=1 AND name='name1') OR (id=2 OR name='name3')"),
    (16, "SELECT * FROM public.person WHERE (id=1 AND name='name1') OR (id=2 AND (name='name2' OR name='name4'))")
  )

  /***AGGREGATIONS***/
  val aggregationQueries = List(
    (1, "SELECT count(id) FROM public.person "),
    (2, "SELECT sum(id) FROM public.person "),
    (3, "SELECT avg(id) FROM public.person "),
    (4, "SELECT max(id) FROM public.person "),
    (5, "SELECT min(id) FROM public.person "),
    (6, "SELECT bit_and(id) FROM public.person"),
    (7, "SELECT bit_or(id) FROM public.person"),
    (8, "SELECT string_agg(city, ' ') FROM public.person"),
    (9, "SELECT name, max(id) FROM public.person group by name, id"),
    (10, "SELECT name AS n, max(id) AS maxim FROM public.person group by name"),
    (11, "SELECT name AS n, max(id) AS maxim FROM public.person group by n"),
    (12, "SELECT name AS n, max(id) FROM public.person group by name, id having max(id) < 7"),
    (13, "SELECT name AS n, max(id) AS maxim FROM public.person group by n, id having max(id) < 7"),
    (14, "SELECT name AS n, max(id) AS maxim FROM public.person group by n, id having maxim < 7") // postgresql doesn't support alias in having clause
  )

  /***SORT***/
  val sortQueries = List(
    (1, "SELECT name, count(id) FROM public.person group by name order by name"),
    (2, "SELECT name, count(id) FROM public.person group by name order by 2"),
    (3, "SELECT name, count(id) FROM public.person group by 1 order by 1"), //spark doesn't support number id in group by clause
    (4, "SELECT count(id) FROM public.person order by count(id)"),
    (5, "SELECT count(id) as count FROM public.person order by count(id)"),
    (6, "SELECT count(id) as count FROM public.person order by count"),
    (7, "SELECT city, count(id) as count FROM public.person group by city order by city ASC, count DESC"),
    (8, "SELECT city, count(id) as count FROM public.person group by city order by 1 ASC, 2 DESC")
  )

  /***SUBQUERIES***/
  val subqueryQueries = List(
    (1, "SELECT name FROM ( select * from public.person) AS t1"),
    (2, "SELECT t1.name FROM ( select * from public.person) AS t1"),
    (3, "SELECT name, (select id from public.person where id=1) as maxid FROM public.person"),
    (4, "SELECT name, (select max(id) as maxidentifier from public.person) as maxid FROM public.person"),
    (5, "SELECT id, name FROM public.person WHERE EXISTS (SELECT NULL)"),
    (6, "SELECT id, name FROM public.person WHERE id IN (SELECT id from public.person where id=1)")
  )

  /***JOIN***/
  val joinQueries = List(
    (1, "SELECT * FROM public.person, public.school "),
    (2, "SELECT p.id, s.id FROM public.person as p, public.school as s"),
    (3, "SELECT p.* FROM public.person as p INNER JOIN public.school as s ON p.city=s.city"),
    (4, "SELECT * FROM public.person as p LEFT JOIN public.school as s ON p.city=s.city"),
    (5, "SELECT * FROM public.person as p FULL OUTER JOIN public.school as s ON p.city=s.city"),
    (6, "SELECT city FROM public.person UNION ALL SELECT city FROM public.school ORDER BY city"),
    (7, "SELECT city FROM public.person INTERSECT SELECT city FROM public.school"),
    (8, "SELECT city FROM public.person EXCEPT SELECT city FROM public.school"),
    (9, "SELECT p.* FROM (select * from public.person) as p INNER JOIN (select * from public.school) as s ON p.city=s.city"),
    (10, "SELECT p.* FROM (select * from public.person where id < 6) as p INNER JOIN (select * from public.school where id < 4) as s ON p.city=s.city"),
    (11, "SELECT p.* FROM (select count(id) as c from public.person where id < 3) as p INNER JOIN (select count(id) as c from public.school where id < 3) as s ON p.c=s.c")
  )
}