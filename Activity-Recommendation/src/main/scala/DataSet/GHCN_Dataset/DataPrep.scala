package DataSet.GHCN_Dataset

/**
  * Created by avalj on 11/12/16.
  */

import scala.io.Source

object DataPrep {

  def main(args: Array[String]) {
    //val stationData = Source.fromFile("./ghcnd-stations.txt")
    var weatherData = Source.fromFile("./weatherData/2000.csv")
    for (i <- 2000 to 2016) {
      println(i)
      weatherData = Source.fromFile("./weatherData/" + i + ".csv")
      for (line <- weatherData.getLines()) {
        val cols_Weather = line.split(",").map(_.trim)
        if (cols_Weather(0).contains("USC0041")) {
          //Texas Station Code
          val file = scala.tools.nsc.io.File("./Temp_data/" + cols_Weather(0) + ".txt")
            .appendAll(cols_Weather(0) + "," + cols_Weather(1) + "," + cols_Weather(2) + "," + cols_Weather(3) + "\n")
        }
      }
    }
    println("Done")
  }
}
