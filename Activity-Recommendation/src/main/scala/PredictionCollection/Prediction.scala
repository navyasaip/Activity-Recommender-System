package PredictionCollection

import java.io.File
import java.nio.file.Paths

import DataSet.GHCN_Dataset.UpdatedData
import Model.{CreateTestData1, CreateTestData2, PressureModel, TemperatureModel}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by avalj on 11/22/16.
  */

object Prediction {

  def getFilename(file: File): String = {
    val path = Paths.get(file.toString)
    path.getFileName.toString
  }

  def deleteOldFiles(temp: Boolean, bar: Boolean): Unit = {
    if (temp && bar) {
      val prediction_old = new File("Prediction").listFiles.filter(_.getName.endsWith(".txt"))
      prediction_old.foreach { file =>
        file.delete()
      }
      println("Old Prediction Deleted!!")
    }
    if (!bar) {
      val trainingData = new File("TrainingData2").listFiles.filter(_.getName.endsWith(".txt"))
      trainingData.foreach { file =>
        file.delete()
      }
      println("Pressure Old Training data  deleted!!")
    }

  }

  def Temperature(Station_ids: Array[String], sc: SparkContext, date_to_predict: String): Unit = {
    val millisec = UpdatedData.creationDate()
    val dateModified = new DateTime(millisec.toLong)
    //UpdatedData.downloadData(1, 10)
    UpdatedData.downloadData(dateModified.getDayOfMonth - 1, dateModified.getMonthOfYear - 1)
    //date and month after which data is required

    deleteOldFiles(true, true) //delete both prediction
    println("Predicting Temperature Now....")
    temp_prediction(date_to_predict, sc, Station_ids)
  }

  def temp_prediction(date_to_predict: String, sc: SparkContext, ids: Array[String]): Unit = {

    val texasData = new File("TexasData").listFiles.filter(_.getName.endsWith(""))
    ids.foreach { id =>

      texasData.foreach { file =>
        val filename = getFilename(file) // stationid_Tmax or tmin or prcp

        if (filename.contains(id)) {
          if (filename.contains("TMAX")) {
            CreateTestData1.testData_prep(date_to_predict, filename, "TMAX")
            TemperatureModel.temp_model(date_to_predict, filename, "TMAX", sc)
          }
          if (filename.contains("TMIN")) {
            CreateTestData1.testData_prep(date_to_predict, filename, "TMIN")
            TemperatureModel.temp_model(date_to_predict, filename, "TMIN", sc)
          }
        }
      }
    }
    println("Completed Temperature Prediction!!")
  }

  def Pressure(Stations_ids: Array[String], sc: SparkContext, date_to_predict: String): Unit = {

    deleteOldFiles(false, false) //delete pressure training data

    /// Pressure prediction
    println("Predicting Pressure Now....")
    pressure_model(date_to_predict, sc, Stations_ids)
    println("Success!!")
  }

  def pressure_model(dateToPredict: String, sc: SparkContext, ids: Array[String]): Unit = {

    val slp_data = new File("DataSet2").listFiles.filter(_.getName.endsWith(".txt"))

    ids.foreach { id =>
      slp_data.foreach { file =>
        val filename = getFilename(file)
        if (filename.contains(id)) {
          CreateTestData2.main(file, dateToPredict)
        }
      }
      val trainingData = new File("TrainingData2").listFiles().filter(_.getName.endsWith(".txt"))
      trainingData.foreach { file =>
        val filename = getFilename(file)
        if (filename.contains(id)) {
          println(filename)
          PressureModel.pressure_prediction(filename, dateToPredict + "0600", sc)
        }
      }
    }
    println("Pressure Prediction Completed")
  }


}
