package Model

import java.util


import org.joda.time.format.DateTimeFormat

import scala.io.Source
import java.io.{File, PrintWriter}

/**
  * Created by avalj on 11/19/16.
  */
object CreateTestData1 {

  def testData_prep(dateToPredict: String, stationId: String, cat: String): Unit = {
    var hashSet = new util.HashSet[String]()
    val date = dateToPredict
    val dtf = DateTimeFormat.forPattern("yyyyMMdd")
    var jodatime = dtf.parseDateTime(date)
    val dtfOut = DateTimeFormat.forPattern("yyyyMMdd")

    for (i <- 1 to 7) {
      jodatime = jodatime.minusDays(1)
      hashSet.add(dtfOut.print(jodatime).substring(4, 8))
    }

    val file = Source.fromFile("./TexasData/" + stationId)
    val writer = new PrintWriter(new File("./TrainingData1/" + cat + "/" + stationId + ".txt"))
    for (line <- file.getLines()) {
      val cols = line.split(",").map(_.trim)
      if (hashSet.contains(cols(1).substring(4, 8))) {
        writer.write(line + "\n")
      }
    }
    writer.close()
  }
}
