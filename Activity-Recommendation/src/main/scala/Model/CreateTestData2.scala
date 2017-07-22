package Model

import java.io.{File, PrintWriter}
import java.nio.file.Paths

import org.joda.time.format.DateTimeFormat

import scala.io.Source

/**
  * Created by avalj on 12/03/16.
  */
object CreateTestData2 {

  def main(slp_file: File, predictDate: String): Unit = {

    val dtf = DateTimeFormat.forPattern("yyyyMMdd")
    var dateObject = dtf.parseDateTime(predictDate)
    val previousDate = dateObject.minusDays(1)
    val prev_date = dtf.print(previousDate) //string

    val slp_data = Source.fromFile(slp_file)
    val testData = slp_data.getLines().filter { line =>
      line.split(",")(2).substring(4, 8).contains(prev_date.substring(4, 8))
    }

    val p = Paths.get(slp_file.toString)
    var f = p.getFileName.toString
    if (testData.nonEmpty) {
      testData.foreach { line =>
        val newFile = scala.tools.nsc.io.File("./TrainingData2/" + f.split("-")(0) + "-" + f.split("-")(1) + ".txt")
          .appendAll(line + "\n")
      }
    }
  }
}
