package DataSet.AddDataToMongoDB

import com.mongodb.DBObject
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.query.dsl.GeoCoords
import com.mongodb.casbah.query.Imports._

import scala.io.Source

/**
  * Created by avalj on 12/07/16.
  */


case class Station(id: String, lat: String, long: String, elev: String)

object ghcn {

  def readGhcnFile(): Seq[Station] = {
    val ghcn = Source.fromFile("./ghcnd-stations.txt").getLines().filter(line => line.contains("USC0041"))
    val stationSeq = ghcn.map { line =>
      val formatedLine = line.toString.replaceAll(" +", " ")
      val parts = formatedLine.split(" ")
      Station(parts(0), parts(1), parts(2), parts(3))
    }
    stationSeq.toSeq
  }

  def toBson(station: Station): DBObject = {
    MongoDBObject(
      "station_id" -> station.id,
      "coordinates" -> GeoCoords(station.lat.toDouble, station.long.toDouble),
      "elevation" -> station.elev
    )
  }

  def main(args: Array[String]): Unit = {
    val station_seq = readGhcnFile()
    val mongoClient = MongoClient("localhost", 27017)

    //Connect to db
    val db = mongoClient("bigdataproject")

    //Create collection
    val stations_collection = db("stations_ghcn")

    //Upload all places in collection interestPoint
    station_seq.foreach { x =>
      stations_collection.insert(toBson(x))
    }

    println("Done: Uploaded all staions in collection stations points")
  }
}
