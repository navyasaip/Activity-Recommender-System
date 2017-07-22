package Model

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.DecisionTree

import scala.io.Source


/**
  * Created by avalj on 11/19/16.
  */
object TemperatureModel {
  def temp_model(predictDate: String, stationId: String, cat: String, sc: SparkContext): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val data = sc.textFile("./TrainingData1/" + cat + "/" + stationId + ".txt")
    val parsedData = data.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(3).toDouble, Vectors.dense(parts(1).toDouble))
    }
    if (!parsedData.isEmpty() && parsedData.count() >= 10) {

      val categoricalFeaturesInfo = Map[Int, Int]()
      val impurity = "variance"
      val maxDepth = 30
      val maxBins = 365 / 7
      val model = DecisionTree.trainRegressor(parsedData.distinct(), categoricalFeaturesInfo, impurity, maxDepth, maxBins)

      val prediction = model.predict(Vectors.dense(predictDate.toDouble))
      val cols = data.map(line => line.split(",")(0))
      val id = cols.collect()

      if (cat.equals("TMAX")) {
        scala.tools.nsc.io.File("./Prediction/" + cat + ".txt")
          .appendAll(id(0) + "," + predictDate + "," + (prediction / 10.0).toString + "\n")

      }
      else //tmin
        scala.tools.nsc.io.File("./Prediction/" + cat + ".txt")
          .appendAll(id(0) + "," + predictDate + "," + (prediction / 10.0).toString + "\n")

    }
  }

  def convert(day: Int, month: Int): Int = {
    val nbreJour = List(31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    var pos = 0
    for (i <- Range(0, month - 1)) {
      pos += nbreJour(i)
    }
    pos + (day - 1)
  }

}


