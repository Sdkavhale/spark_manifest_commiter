package org.example

import org.apache.spark.sql.SparkSession
import org.json4s.ParserUtil.{ParseException, quote}

object QueryValidation extends App {

  private val sparkSession = SparkSession.builder().master("local[4]").appName("QV").getOrCreate()

  private val query = """SELECT COUNT(1)
                |FROM clientele_transactions
                |WHERE DATE < date_sub(current_date(), 5)
                |AND FK_CUSTOMER_ID IN (SELECT PK_CUSTOMER_ID FROM clientele)
                |
                |""".stripMargin

  println(validateQuery(query))


  private def validateQuery(query: String): Boolean = {
    val parser = sparkSession.sessionState.sqlParser
    try {
      parser.parsePlan(query)
      true
    } catch {
      case ex: ParseException => false
    }
  }

}
