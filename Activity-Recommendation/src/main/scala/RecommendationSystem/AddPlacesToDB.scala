package RecommendationSystem

import com.mongodb.DBObject
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.query.dsl.GeoCoords
import com.mongodb.casbah.query.Imports._

import scala.io.Source

/**
  * Created by avalj on 12/08/16.
  */

case class InterestPoints(id: String, lat: String, long: String, name: String, tourism: String, amenity: String, sport: String)

object AddPlacesToDB {

  def ReadPlacesFile(): Seq[InterestPoints] = {
    val file = Source.fromFile("./InterestPoints.txt").getLines
    //file.take(5).foreach(println)
    var tourism = "None"
    var amenity = "None"
    var sport = "None"
    val placesSeq = file.map { line =>
      val formatedLine = line.replace("(", "").replace(")", "")
      //println(formatedLine)
      val data = formatedLine.split(",")
      val id = data(0).drop(1)
      val latitude = data(1)
      val longitude = data(2)
      val name = data(3)
      val activities = data(8).split(";")

      if (!activities(0).isEmpty) {
        tourism = activities(0)
      }
      if (activities.length >= 2 && !activities(1).isEmpty) {
        amenity = activities(1)
      }
      if (activities.length > 2) {
        if (!activities(2).isEmpty) {
          sport = activities(2)
        }
      }
      InterestPoints(id, latitude, longitude, name, tourism, amenity, sport)

    }
    placesSeq.toSeq
  }

  def toBson(interestPoint: InterestPoints): DBObject = {

    MongoDBObject(
      "osm_id" -> interestPoint.id,
      "name" -> interestPoint.name,
      "tourism" -> interestPoint.tourism,
      "amenity" -> interestPoint.amenity,
      "sport" -> interestPoint.sport,
      "geometry" ->
        MongoDBObject(
          "type" -> "Point",
          "coordinates" -> GeoCoords(interestPoint.lat.toDouble, interestPoint.long.toDouble)
        )
    )
  }

  def main(args: Array[String]): Unit = {

    val interest_points_seq = ReadPlacesFile()

    val mongoClient = MongoClient("localhost", 27017)
    //Connect to db
    val db = mongoClient("bigdataproject")
    //Create collection
    val places_collection = db("interestPoint")
    //Upload all places in collection interestPoint
    interest_points_seq.foreach { x =>
      places_collection.insert(toBson(x))
    }
    println("Done: Uploaded all data points in collection interestPoint points")
  }

}
