package com.amadeus.app

import org.scalatra._
import scalikejdbc._

class MyScalatraServlet extends Scalatra_appStack {

  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(
    enabled = true,
    singleLineMode = false,
    printUnprocessedStackTrace = false,
    stackTraceDepth= 15,
    logLevel = 'debug,
    warningEnabled = false,
    warningThresholdMillis = 3000L,
    warningLogLevel = 'warn
  )

  get("/") {
    <html>
      <body>
        <h1>Hello, people!</h1>
      </body>
    </html>
  }

  get("/dbconn") {
    // Capture GET parameter
    val carrier = params.get("carrier").map(_.toString).getOrElse("")

    // Establish JDBC connection to Impala
    Class.forName("com.cloudera.impala.jdbc41.Driver")
    ConnectionPool.singleton("jdbc:impala://127.0.0.1:21050;AuthMech=0;","", "")

    // Run SQL Query
    val res: Option[Int] = DB readOnly { implicit session =>
      SQL("SELECT sum(pax_delta) as pax_delta from nh_ti_vistana_v2_4_p46_od1.booking_ond_activity_carrier_v24 where operating_carrier = ?").bind(carrier).map(rs => rs.int("pax_delta")).single.apply()
    }

    s"Pax count for ${carrier} is ${res.map(_.toInt).getOrElse(0)}"
  }

}
