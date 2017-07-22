package Application

import RecommendationSystem._
import org.joda.time.DateTime
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL.WithDouble._

/**
  * Created by avalj on 12/09/16.
  */
object MyApp {

  def main(args: Array[String]): Unit = {
    val Point_Of_Interest = "Austin"   //User location ( we can get this from browser when implementing web-app)
    val maxDistance = 1.0
    val todayDate = new DateTime()
    val nextDate = todayDate.plusDays(1)
    println("For: " + nextDate)

    val Weather_data = WeatherCollection.weather(Point_Of_Interest, maxDistance, nextDate)

    val weather = Weather_data.map(w => (w.Place, w.Temp_max, w.Temp_min, w.weather_type))
    weather.sortBy(w => w._1).foreach(println)
    val place_of_weather = weather(0)._1
    println(Weather_data(0))
    val placeCoord = WeatherCollection.getGeoCoordinate(place_of_weather)
    val nearInterest = InterestPoint.getNearestInterestPoints(placeCoord, maxDistance)
    val recommendation = Recommendation.Recommend(nearInterest, Weather_data(0))
    val array_of_points = recommendation.flatMap(r => r.places)
    val jsonObj = convertAllPointsToGeoJson(array_of_points, Weather_data(0))
    jsonObj.foreach(json =>
      println(pretty(render(jsonObj(0))))
    )
    val rPoints = array_of_points.map(p =>
      (p.latitude, p.longtitude, p.name, p.tourism, p.cultural, p.parks, p.leisure, p.sport, p.nightlife)
    )
  }

  //Convert InterestPoint_out array to Array[String] GeoJSON format
  def convertAllPointsToGeoJson(arr: Array[InterestPoint_reformed], weather: Weather): Array[JObject] = {
    arr.map(x => convertToGeoJson(x, weather))
  }

  //Convert InterestPoint_out data structure to GeoJson
  def convertToGeoJson(point: InterestPoint_reformed, weather: Weather): JObject = {
    val coordinates = List(point.latitude, point.longtitude)
    val temperature = (weather.Temp_max + weather.Temp_min) / 2.0
    val bar = weather.weather_type
    val json =
      ("type" -> "Feature") ~
        ("geometry" ->
          ("type" -> "Point") ~
            ("coordinates" -> coordinates)
          ) ~
        ("properties" ->
          ("name" -> point.name) ~
            ("tourism" -> point.tourism) ~
            ("cultural" -> point.cultural) ~
            ("parks" -> point.parks) ~
            ("leisure" -> point.leisure) ~
            ("nightlife" -> point.nightlife) ~
            ("sport" -> point.sport) ~
            ("temperature" -> temperature) ~
            ("bar" -> bar)
          )
    json
  }


}
