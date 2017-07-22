package RecommendationSystem

import com.mongodb.DBObject
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.query.Imports._
import org.json4s._
import org.json4s.native.JsonMethods._


/**
  * Created by avalj on 12/09/16.
  */

case class InterestPoint_out(id: String, latitude: Double, longtitude: Double, name: String, tags: List[String])

case class InterestPoint_reformed(id: String, latitude: Double, longtitude: Double,
                                  name: String, tourism: Option[String], cultural: Option[String],
                                  parks: Option[String], leisure: Option[String], nightlife: Option[String], sport: Option[String])

object InterestPoint {

  def getNearestInterestPoints(location: Location, radius: Double): Array[InterestPoint_reformed] = {
    //Connect to db and get interesting places
    val mongoClient = MongoClient("localhost", 27017)
    val db = mongoClient("bigdataproject")
    val places_collection = db("interestPoint")

    //Location coordinates
    val lat = location.latitude
    val long = location.longtitude

    val points = "geometry.coordinates" $near(lat, long) $maxDistance radius
    val result = places_collection.find(points).toArray
    result.map { x => fromBson(x) }.filter { x => filterNoneTags(x) }
  }

  def fromBson(o: DBObject): InterestPoint_reformed = {
    val point = InterestPoint_out(
      o.as[String]("osm_id"),
      readCoordinates(o.as[DBObject]("geometry").toString)._1,
      readCoordinates(o.as[DBObject]("geometry").toString)._2,
      o.as[String]("name"),
      List(o.as[String]("tourism"), o.as[String]("amenity"), o.as[String]("sport"))
    )
    reformatTags(point)
  }

  def filterNoneTags(point: InterestPoint_reformed): Boolean = {
    !(point.tourism.isEmpty && point.cultural.isEmpty && point.parks.isEmpty
      && point.leisure.isEmpty && point.nightlife.isEmpty && point.sport.isEmpty)
  }

  //Convert coordinates from Bson database output
  def readCoordinates(ob: String): (Double, Double) = {
    val data = parse(ob)
    val coordinates_string = compact(render(data \ "coordinates"))
    val latitude = coordinates_string.split(",")(0).drop(1).toDouble
    val longtitude = coordinates_string.split(",")(1).dropRight(1).toDouble
    (latitude, longtitude)
  }

  // Reformat tags by 6 categories and return new case class InterestPoit_reform
  def reformatTags(point: InterestPoint_out): InterestPoint_reformed = {
    val tags = checkTag(point.tags)
    InterestPoint_reformed(point.id, point.latitude, point.longtitude, point.name, tags(0), tags(1), tags(2), tags(3), tags(4), tags(5))
  }

  def checkTag(tags: List[String]): Array[Option[String]] = {
    var reformated_tags: Array[Option[String]] = Array(None, None, None, None, None, None)
    tags.foreach { x =>
      if (List("attraction", "information", "viewpoint").contains(x)) reformated_tags(0) = Some(x) // tourism tags
      if (List("arts_centre", "planetarium", "theatre").contains(x)) reformated_tags(1) = Some(x) // cultural tags
      if (List("picnic_site").contains(x)) reformated_tags(2) = Some(x) // parks tags
      if (List("theme_park", "zoo", "cinema", "gym", "marketplace", "restaurant").contains(x)) reformated_tags(3) = Some(x) // leisure tags
      if (List("nightclub", "casino", "striplub", "cinema", "bar", "pub").contains(x)) reformated_tags(4) = Some(x) // nightlife tags
      if (List("9pin", "10pin", "american_football", "base", "baseball", "basketball", "bmx", "boxing", "canoe", "cliff_diving",
        "climbing_adventure", "roller_skating", "scuba_diving", "sailing", "safety_training", "skateboard", "skiing", "surfing",
        "swimming", "water_ski").contains(x))
        reformated_tags(5) = Some(x) // sport tags
    }
    reformated_tags
  }
}
