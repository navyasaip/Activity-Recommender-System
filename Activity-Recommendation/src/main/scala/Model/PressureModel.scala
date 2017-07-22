package Model

import org.apache.commons.io.FilenameUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest


/**
  * Created by avalj on 12/04/16.
  */
object PressureModel {

  def pressure_prediction(filename: String, dateToPredict: String, sc: SparkContext): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val slp_trainingData = sc.textFile("./TrainingData2/" + filename)
    val slpRdd = slp_trainingData.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(4).toDouble, Vectors.dense(parts(2).toDouble))
    }

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 5
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 10
    // Use more in practice.
    val featureSubsetStrategy = "auto"
    // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 5

    val model = RandomForest.trainClassifier(slpRdd.distinct(), numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    val prediction = model.predict(Vectors.dense(dateToPredict.toDouble))
    scala.tools.nsc.io.File("./Prediction/pressure.txt")
      .appendAll(FilenameUtils.removeExtension(filename) + "," + dateToPredict + "," + prediction + "\n")
  }

}
