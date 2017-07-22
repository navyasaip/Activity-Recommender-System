package DataSet.AddDataToMongoDB

import com.mongodb.DBObject
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.query.dsl.GeoCoords
import com.mongodb.casbah.query.Imports._

import scala.io.Source

/**
  * Created by avalj on 12/08/16.
  */



object isd {

  def readIsdFile(): Seq[Station] = {
    val ghcn = Source.fromFile("./ISD-Inv.txt").getLines().filter(line => line.contains("US TX"))
    val stationSeq = ghcn.map { line =>
      val formatedLine = line.toString.replaceAll(" +", " ").replace("+", "")
      val size = formatedLine.split(" ").length
      val parts = formatedLine.split(" ")
      Station(parts(0) + "-" + parts(1), parts(size - 3), parts(size - 2), parts(size - 1).replace("+", "").replace("-", ""))
    }
    stationSeq.toSeq
  }

  def toBson(station: Station): DBObject = {
    MongoDBObject(
      "station_id" -> station.id,
      "coordinates" -> GeoCoords(station.lat.toDouble/1000.0, station.long.toDouble/1000.0),
      "elevation" -> (station.elev.toDouble/10.0).toString
    )
  }

  def main(args: Array[String]): Unit = {
    val station_seq = readIsdFile()

    val mongoClient = MongoClient("localhost", 27017)
    //Connect to db
    val db = mongoClient("bigdataproject")
    //Create collection
    val stations_collection = db("stations_isd")
    //Upload all places in collection interestPoint
    station_seq.foreach { x =>
      stations_collection.insert(toBson(x))
    }
    println("Done: Uploaded all staions in collection stations points")
  }

}
